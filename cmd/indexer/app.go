package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"whale-watcher/pkg/config"
	"whale-watcher/pkg/model"
	"whale-watcher/pkg/repository"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// =================================================================================
// Configuration & Constants
// =================================================================================

const (
	logChannelCapacity     = 100
	logChannelBufferSize   = 2
	defaultWorkerCount     = 2
	progressReportInterval = 100
	blockLookbackDistance  = 10
	httpTimeout            = 10 * time.Second
	maxRetries             = 3
	retryBaseDelay         = time.Second
)

// BlockStatus represents the relationship between the new block and our local chain.
type BlockStatus int

const (
	StatusCanonical BlockStatus = iota // Normal continuation of the chain
	StatusGap                          // Block is from the future (missing intermediate blocks)
	StatusReorg                        // Block conflicts with local history (fork detected)
)

// =================================================================================
// Application Structure
// =================================================================================

// App represents the main indexer application.
// It manages the database, blockchain connection, and synchronization logic.
type App struct {
	Config    *config.Config
	DB        *gorm.DB
	EthClient *ethclient.Client
	BlockRepo repository.BlockRepository
	LogChan   chan string // Dedicated channel for non-blocking logging
}

// Initialize bootstraps the application dependencies.
func Initialize(cfg *config.Config) (*App, error) {
	db, err := initDB(cfg.DB_DSN)
	if err != nil {
		return nil, fmt.Errorf("database connection failed: %w", err)
	}
	fmt.Println("✅ Database connection successful")

	repo := repository.NewBlockRepo(db)

	client, err := ethclient.Dial(cfg.EthWSURL)
	if err != nil {
		return nil, fmt.Errorf("websocket connection failed: %w", err)
	}

	return &App{
		Config:    cfg,
		DB:        db,
		EthClient: client,
		BlockRepo: repo,
		LogChan:   make(chan string, logChannelCapacity),
	}, nil
}

func initDB(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	// Ensure the schema is up to date
	if err := db.AutoMigrate(&model.SQLBlock{}); err != nil {
		return nil, err
	}
	return db, nil
}

// =================================================================================
// Main Execution Loop
// =================================================================================

// Run starts the application components: logger, backfiller, and listener.
func (a *App) Run() {
	// 1. Start the async logger consumer
	go a.startLogger()

	// 2. Start initial backfill in background
	// Using a separate goroutine prevents blocking the startup flow.
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := a.Backfill(ctx); err != nil {
			log.Printf("⚠️ Error during initial backfill: %v", err)
		}
	}()

	// 3. Start real-time block monitoring
	go a.startWebSocketListener()

	// 4. Block main thread until interrupt signal
	a.waitForShutdown()
}

func (a *App) waitForShutdown() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("🛑 Received shutdown signal, closing service...")
	// TODO: Implement graceful shutdown for DB and Workers here
	log.Println("👋 Bye!")
}

// =================================================================================
// Real-time Listener Logic
// =================================================================================

func (a *App) startWebSocketListener() {
	headers := make(chan *types.Header)

	// Subscribe to new block headers
	sub, err := a.EthClient.SubscribeNewHead(context.Background(), headers)
	if err != nil {
		log.Fatal("Subscription failed:", err)
	}

	// Loop indefinitely to process incoming headers
	for {
		select {
		case err := <-sub.Err():
			log.Fatal("Subscription interrupted with error:", err)
		case header := <-headers:
			// NOTE: ProcessBlock is called synchronously to ensure sequential processing
			// of headers, which simplifies reorg handling.
			a.ProcessBlock(header)
		}
	}
}

// ProcessBlock is the main entry point for handling a new block header.
// It determines if the block is canonical, a gap, or a reorg, and acts accordingly.
func (a *App) ProcessBlock(header *types.Header) {
	ctx := context.Background()

	// 1. Fetch full block data
	block, err := a.EthClient.BlockByHash(ctx, header.Hash())
	if err != nil {
		log.Printf("❌ Failed to fetch block body #%s: %v", header.Number.String(), err)
		return
	}

	// 2. Get local state
	localTip, err := a.BlockRepo.GetLastBlock()
	if err != nil {
		log.Printf("❌ Failed to get local tip: %v", err)
		return
	}

	// 3. Determine status
	status := determineBlockStatus(localTip, block)

	// 4. Handle based on status
	switch status {
	case StatusCanonical:
		// Happy path: just save the block
		if err := a.saveBlock(block); err != nil {
			log.Printf("❌ Failed to save canonical block: %v", err)
		}

	case StatusGap:
		// Gap detected (e.g., local: 100, new: 105).
		// We save the future block and assume the Backfill process (or a separate repair job)
		// will fill the missing blocks (101-104).
		log.Printf("📥 Gap detected (Local: #%d, New: #%d). Saving future block.", localTip.Height, block.NumberU64())
		if err := a.saveBlock(block); err != nil {
			log.Printf("❌ Failed to save gap block: %v", err)
		}

	case StatusReorg:
		// Reorg detected (e.g., local: 100, new: 100 but diff hash).
		log.Printf("⚠️ Reorg detected at block #%s. Starting resolution...", block.Number().String())
		if err := a.ResolveReorg(ctx, localTip, block); err != nil {
			log.Printf("❌ Reorg resolution failed: %v", err)
		} else {
			log.Printf("✅ Reorg resolved up to block #%s", block.Number().String())
		}
	}
}

// determineBlockStatus analyzes the relationship between local tip and new block.
func determineBlockStatus(local *model.SQLBlock, newBlock *types.Block) BlockStatus {
	// Case 1: First run (empty DB) -> Treat as canonical
	if local == nil {
		return StatusCanonical
	}

	newHeight := newBlock.NumberU64()
	newParent := newBlock.ParentHash().Hex()

	// Case 2: Perfect successor
	if newHeight == local.Height+1 && newParent == local.Hash {
		return StatusCanonical
	}

	// Case 3: Block is from the future (Gap)
	// NOTE: We do not trigger reorg logic for gaps.
	if newHeight > local.Height+1 {
		return StatusGap
	}

	// Case 4: Everything else (Height <= LocalHeight OR Parent mismatch) is a Reorg
	return StatusReorg
}

// =================================================================================
// Reorg Handling Logic
// =================================================================================

// ResolveReorg handles chain reorganizations by finding a common ancestor
// and rewriting history.
func (a *App) ResolveReorg(ctx context.Context, localTip *model.SQLBlock, newHead *types.Block) error {
	// 1. Find the point where our chain diverged from the canonical chain
	commonAncestorHeight, err := a.findCommonAncestor(ctx, localTip.Height)
	if err != nil {
		return fmt.Errorf("failed to find common ancestor: %w", err)
	}

	// 2. Fill the gap from the ancestor to the new head
	// This overwrites any invalid blocks in our DB with canonical ones.
	startFix := commonAncestorHeight + 1
	endFix := newHead.NumberU64()

	if startFix <= endFix {
		log.Printf("🔄 Repairing chain from #%d to #%d", startFix, endFix)
		// Reuse sequential logic here for simplicity and safety during repair
		for h := startFix; h <= endFix; h++ {
			if err := a.fetchAndSaveBlock(ctx, int64(h)); err != nil {
				return err
			}
		}
	}
	return nil
}

// findCommonAncestor walks backwards from current height to find a block hash match.
func (a *App) findCommonAncestor(ctx context.Context, startHeight uint64) (uint64, error) {
	currentHeight := startHeight

	for currentHeight > 0 {
		// A. Fetch canonical block from chain
		onChainBlock, err := a.EthClient.BlockByNumber(ctx, big.NewInt(int64(currentHeight)))
		if err != nil {
			return 0, fmt.Errorf("rpc failed for #%d: %w", currentHeight, err)
		}

		// B. Fetch local block
		localBlock, err := a.BlockRepo.GetByHeight(currentHeight)
		if err != nil {
			return 0, fmt.Errorf("db failed for #%d: %w", currentHeight, err)
		}

		// C. Compare Hashes
		if localBlock.Hash != onChainBlock.Hash().Hex() {
			log.Printf("🔪 Fixing block #%d (Local: %s != Chain: %s)",
				currentHeight, localBlock.Hash, onChainBlock.Hash().Hex())

			// Overwrite the bad block immediately
			if err := a.saveBlock(onChainBlock); err != nil {
				return 0, err
			}
			currentHeight--
		} else {
			// Match found! This is our common ancestor.
			log.Printf("⚓ Common ancestor found at #%d", currentHeight)
			return currentHeight, nil
		}
	}
	return 0, nil
}

// =================================================================================
// Backfill & Sync Logic
// =================================================================================

// Backfill checks for missing historical data and starts synchronization.
func (a *App) Backfill(ctx context.Context) error {
	log.Println("🔍 Checking data continuity...")

	localHeight, err := a.BlockRepo.GetMaxHeight()
	if err != nil {
		return fmt.Errorf("failed to query local height: %w", err)
	}

	remoteHeight, err := a.EthClient.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to query chain height: %w", err)
	}

	log.Printf("📊 Status check: Local [#%d] vs Chain [#%d]", localHeight, remoteHeight)

	// Optimization: Skip history on fresh install
	if localHeight == 0 && remoteHeight > blockLookbackDistance {
		log.Printf("⚠️ First run, skipping history. Syncing last %d blocks.", blockLookbackDistance)
		localHeight = remoteHeight - blockLookbackDistance
	}

	if localHeight >= remoteHeight {
		log.Println("✅ Data already synced.")
		return nil
	}

	return a.runWorkerPool(ctx, localHeight, remoteHeight)
}

// runWorkerPool manages concurrent workers to fetch missing blocks.
func (a *App) runWorkerPool(ctx context.Context, startHeight, endHeight uint64) error {
	missingCount := endHeight - startHeight
	log.Printf("⚡️ Starting backfill for %d blocks...", missingCount)

	// Buffered channel to hold jobs
	jobs := make(chan uint64, defaultWorkerCount*logChannelBufferSize)
	var wg sync.WaitGroup

	// 1. Start Workers
	for i := 0; i < defaultWorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			a.workerTask(ctx, workerID, jobs)
		}(i)
	}

	// 2. Dispatch Jobs
	// Push jobs to channel until done or context cancelled
	go func() {
		for h := startHeight + 1; h <= endHeight; h++ {
			select {
			case <-ctx.Done():
				return
			case jobs <- h:
			}
		}
		close(jobs)
	}()

	// 3. Wait for completion
	wg.Wait()
	log.Println("✅ Backfill complete!")
	return nil
}

// workerTask is the logic loop for a single worker.
func (a *App) workerTask(ctx context.Context, workerID int, jobs <-chan uint64) {
	for height := range jobs {
		if err := a.fetchAndSaveBlockWithRetry(ctx, int64(height)); err != nil {
			a.AsyncLog("❌ [Worker %d] Failed #%d: %v", workerID, height, err)
		} else {
			if height%progressReportInterval == 0 {
				a.AsyncLog("✅ [Worker %d] Synced #%d", workerID, height)
			}
		}
	}
}

// fetchAndSaveBlockWithRetry attempts to fetch a block with exponential backoff.
func (a *App) fetchAndSaveBlockWithRetry(ctx context.Context, height int64) error {
	for i := 0; i < maxRetries; i++ {
		err := a.fetchAndSaveBlock(ctx, height)
		if err == nil {
			return nil
		}

		// Calculate delay: 1s, 2s, 4s...
		delay := retryBaseDelay * time.Duration(1<<i)
		a.AsyncLog("⚠️ Retry %d/%d for block #%d (Error: %v)", i+1, maxRetries, height, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Retry after delay
		}
	}
	return fmt.Errorf("exceeded max retries for block #%d", height)
}

// =================================================================================
// Helpers & Utilities
// =================================================================================

func (a *App) fetchAndSaveBlock(ctx context.Context, height int64) error {
	block, err := a.EthClient.BlockByNumber(ctx, big.NewInt(height))
	if err != nil {
		return err
	}
	return a.saveBlock(block)
}

func (a *App) saveBlock(block *types.Block) error {
	// Convert Go-Ethereum type to our SQL model
	sqlBlock := model.SQLBlock{
		Height:    block.NumberU64(),
		Hash:      block.Hash().Hex(),
		Timestamp: time.Unix(int64(block.Time()), 0),
		TxCount:   len(block.Transactions()),
		BaseFee:   "0",
	}
	if block.BaseFee() != nil {
		sqlBlock.BaseFee = block.BaseFee().String()
	}

	return a.BlockRepo.Save(&sqlBlock)
}

func (a *App) startLogger() {
	for msg := range a.LogChan {
		log.Println(msg)
	}
}

func (a *App) AsyncLog(format string, v ...interface{}) {
	select {
	case a.LogChan <- fmt.Sprintf(format, v...):
	default:
		// Drop log if channel is full to prevent blocking
	}
}
