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

// Configuration constants
const (
	logChannelCapacity     = 100
	logChannelBufferSize   = 2
	defaultWorkerCount     = 2
	progressReportInterval = 100
	blockLookbackDistance  = 10
	httpTimeout            = 10 * time.Second
	logMessageFormat       = "⚠️ Error during backfill process: %v (program will continue to start)"
)

// App represents the main application with blockchain monitoring capabilities
type App struct {
	Config    *config.Config
	DB        *gorm.DB
	EthClient *ethclient.Client
	BlockRepo repository.BlockRepository
	LogChan   chan string
}

// Initialize creates a new App instance with required connections
func Initialize(cfg *config.Config) (*App, error) {
	db, err := initDB(cfg.DB_DSN)
	if err != nil {
		log.Fatal("Database connection failed:", err)
	}
	fmt.Println("✅ Database connection successful")

	repo := repository.NewBlockRepo(db)

	client, err := ethclient.Dial(cfg.EthWSURL)
	if err != nil {
		log.Fatal("WebSocket connection failed:", err)
	}

	return &App{
		Config:    cfg,
		DB:        db,
		EthClient: client,
		BlockRepo: repo,
		LogChan:   make(chan string, logChannelCapacity),
	}, nil
}

// initDB establishes database connection and runs migrations
func initDB(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	db.AutoMigrate(&model.SQLBlock{})
	return db, nil
}

// Run starts the main event loop with block listener and backfill process
func (a *App) Run() {
	go a.startLogger()

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Perform initial backfill of missing blocks
		if err := a.Backfill(ctx); err != nil {
			log.Printf(logMessageFormat, err)
		}
	}()
	// Start listening for new blocks
	go a.startWebSocketListener()

	// Wait for shutdown signal
	a.waitForShutdown()
}

// waitForShutdown blocks until a shutdown signal is received
func (a *App) waitForShutdown() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("🛑 Received shutdown signal, closing service...")
	log.Println("👋 Bye!")
}

// startWebSocketListener subscribes to new block headers and processes them
func (a *App) startWebSocketListener() {
	headers := make(chan *types.Header)

	sub, err := a.EthClient.SubscribeNewHead(context.Background(), headers)
	if err != nil {
		log.Fatal("Subscription failed:", err)
	}

	for {
		select {
		case err := <-sub.Err():
			log.Fatal("Subscription interrupted with error:", err)

		case header := <-headers:
			a.ProcessBlock(header)
		}
	}
}

// ProcessBlock fetches and stores a new block
func (a *App) ProcessBlock(header *types.Header) {
	ctx := context.Background()
	block, err := a.EthClient.BlockByHash(ctx, header.Hash())
	if err != nil {
		log.Printf("❌ Failed to fetch block body #%s: %v", header.Number.String(), err)
		return
	}
	newHeight := block.NumberU64()
	newParentHash := block.ParentHash().Hex()
	localTip, err := a.BlockRepo.GetLastBlock()
	if err != nil {
		log.Printf("❌ Failed to get local tip height: %v", err)
		return
	}
	if localTip == nil {
		a.saveBlock(block)
		return
	}
	if newHeight == localTip.Height+1 && newParentHash == localTip.Hash {
		a.saveBlock(block)
		return
	}
	log.Printf("⚠️ Detected chain reorganization at block #%s", block.Number().String())
	if err := a.ResolveReorg(ctx, localTip, block); err != nil {
		log.Printf("❌ Repairing failed: %v", err)
	}
	log.Printf("✅ Repair complete up to block #%s", block.Number().String())

}

func (a *App) ResolveReorg(ctx context.Context, localTip *model.SQLBlock, newHead *types.Block) error {
	currentHeight := localTip.Height
	for {
		if currentHeight == 0 {
			break
		}
		onChainBlock, err := a.EthClient.BlockByNumber(ctx, big.NewInt(int64(currentHeight)))
		if err != nil {
			return fmt.Errorf("failed to fetch block #%d from chain: %w", currentHeight, err)
		}
		localBlock, err := a.BlockRepo.GetByHeight(currentHeight)
		if err != nil {
			return fmt.Errorf("failed to fetch local block #%d: %w", currentHeight, err)
		}
		if localBlock.Hash != onChainBlock.Hash().Hex() {
			log.Printf("🔄 Replacing local block #%d (hash: %s) with on-chain hash: %s", currentHeight, localBlock.Hash, onChainBlock.Hash().Hex())
			if err := a.saveBlock(onChainBlock); err != nil {
				return fmt.Errorf("failed to save corrected block #%d: %w", currentHeight, err)
			}
			currentHeight--
		} else {
			break
		}
	}
	startBackfill := localTip.Height + 1
	endBackfill := newHead.NumberU64()
	if startBackfill <= endBackfill {
		log.Printf("🔄 Backfilling blocks from #%d to #%d after reorg", startBackfill, endBackfill)
		for h := startBackfill; h <= endBackfill; h++ {
			if err := a.fetchAndSaveBlock(ctx, int64(h)); err != nil {
				return err
			}
		}
	}
	return nil
}

// blockToSQLBlock converts an Ethereum block to a database-ready format
func (a *App) blockToSQLBlock(block *types.Block) model.SQLBlock {
	baseFee := "0"
	if block.BaseFee() != nil {
		baseFee = block.BaseFee().String()
	}

	return model.SQLBlock{
		Height:    block.NumberU64(),
		Hash:      block.Hash().Hex(),
		Timestamp: time.Unix(int64(block.Time()), 0),
		TxCount:   len(block.Transactions()),
		BaseFee:   baseFee,
	}
}

func (a *App) saveBlock(block *types.Block) error {
	sqlBlock := a.blockToSQLBlock(block)
	return a.BlockRepo.Save(&sqlBlock)
}

// Backfill synchronizes historical blocks that were missed
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

	// First run: skip to recent blocks instead of syncing entire history
	if localHeight == 0 && remoteHeight > blockLookbackDistance {
		log.Printf("⚠️ First run, skipping historical data, only syncing last %d blocks", blockLookbackDistance)
		localHeight = remoteHeight - blockLookbackDistance
	}

	if localHeight >= remoteHeight {
		log.Println("✅ Data already synced, no backfill needed")
		return nil
	}

	return a.syncMissingBlocks(ctx, localHeight, remoteHeight)
}

// syncMissingBlocks fills in blocks between local and remote heights using worker pool
func (a *App) syncMissingBlocks(ctx context.Context, localHeight, remoteHeight uint64) error {
	missingCount := remoteHeight - localHeight
	log.Printf("⚡️ Detected %d blocks behind, starting sync...", missingCount)

	blockChan := make(chan uint64, defaultWorkerCount*logChannelBufferSize)
	var wg sync.WaitGroup

	// Start worker goroutines
	for workerID := 0; workerID < defaultWorkerCount; workerID++ {
		wg.Add(1)
		go a.blockSyncWorker(ctx, workerID, blockChan, &wg)
	}

	// Distribute blocks to workers
	for blockNumber := localHeight + 1; blockNumber <= remoteHeight; blockNumber++ {
		select {
		case <-ctx.Done():
			close(blockChan)
			return ctx.Err()
		case blockChan <- blockNumber:
		}
	}

	close(blockChan)
	wg.Wait()
	log.Println("✅ Backfill complete! All historical blocks saved.")
	return nil
}

// blockSyncWorker processes blocks from the channel
func (a *App) blockSyncWorker(ctx context.Context, workerID int, blockChan chan uint64, wg *sync.WaitGroup) {
	defer wg.Done()

	for blockNumber := range blockChan {
		if err := a.fetchAndSaveBlockWithRetry(ctx, int64(blockNumber)); err != nil {
			a.AsyncLog("❌ [Worker %d] Failed #%d: %v", workerID, blockNumber, err)
		} else {
			// Log progress at intervals
			if blockNumber%progressReportInterval == 0 {
				a.AsyncLog("✅ [Worker %d] Synced #%d", workerID, blockNumber)
			}
		}
	}
}

func (a *App) fetchAndSaveBlockWithRetry(ctx context.Context, height int64) error {
	maxRetries := 3
	baseDelay := time.Second

	for i := 0; i < maxRetries; i++ {
		err := a.fetchAndSaveBlock(ctx, height)
		if err == nil {
			return nil
		}

		a.AsyncLog("❌ Retry %d for block #%d failed: %v", i+1, height, err)
		delay := baseDelay * time.Duration(1<<i) // Exponential backoff

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next retry
		}
	}
	return fmt.Errorf("failed to fetch and save block #%d after %d retries", height, maxRetries)
}

// fetchAndSaveBlock retrieves a single block and persists it
func (a *App) fetchAndSaveBlock(ctx context.Context, height int64) error {
	block, err := a.EthClient.BlockByNumber(ctx, big.NewInt(height))
	if err != nil {
		return err
	}

	sqlBlock := a.blockToSQLBlock(block)
	return a.BlockRepo.Save(&sqlBlock)
}

// startLogger continuously processes log messages from LogChan
func (a *App) startLogger() {
	for msg := range a.LogChan {
		log.Println(msg)
	}
}

// AsyncLog sends a formatted message to the log channel without blocking
func (a *App) AsyncLog(format string, v ...interface{}) {
	select {
	case a.LogChan <- fmt.Sprintf(format, v...):
	default:
		// Channel full - discard to prevent blocking business logic
	}
}
