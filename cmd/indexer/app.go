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
			go a.ProcessBlock(header)
		}
	}
}

// ProcessBlock fetches and stores a new block
func (a *App) ProcessBlock(header *types.Header) {
	block, err := a.EthClient.BlockByHash(context.Background(), header.Hash())
	if err != nil {
		log.Printf("❌ Failed to fetch block body #%s: %v", header.Number.String(), err)
		return
	}

	fmt.Printf("🧱 New block: #%s | Hash: %s | Txs: %d\n",
		block.Number().String(), block.Hash().Hex(), len(block.Transactions()))

	sqlBlock := a.blockToSQLBlock(block)
	if err := a.BlockRepo.Save(&sqlBlock); err != nil {
		log.Printf("❌ Database save failed: %v", err)
	} else {
		log.Printf("💾 Saved to DB (ID: %d)", sqlBlock.ID)
	}
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
		if err := a.fetchAndSaveBlock(ctx, int64(blockNumber)); err != nil {
			a.AsyncLog("❌ [Worker %d] Failed #%d: %v", workerID, blockNumber, err)
		} else {
			// Log progress at intervals
			if blockNumber%progressReportInterval == 0 {
				a.AsyncLog("✅ [Worker %d] Synced #%d", workerID, blockNumber)
			}
		}
	}
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
