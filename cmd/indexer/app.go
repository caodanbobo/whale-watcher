package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
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

type App struct {
	Config    *config.Config
	DB        *gorm.DB
	EthClient *ethclient.Client
	BlockRepo repository.BlockRepository
}

func Initialize(cfg *config.Config) (*App, error) {

	db, err := initDB(cfg.DB_DSN)

	if err != nil {
		log.Fatal("数据库连接失败:", err)
	}
	fmt.Println("✅ 数据库连接成功")

	repo := repository.NewBlockRepo(db)

	client, err := ethclient.Dial(cfg.EthWSURL)

	if err != nil {
		log.Fatal("WebSocket 连接失败:", err)
	}
	return &App{
		cfg, db, client, repo,
	}, nil
}

func initDB(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	db.AutoMigrate(&model.SQLBlock{})
	return db, nil
}

func (a *App) Run() {
	go a.startWebSocketLisener()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("🛑 收到退出信号，正在关闭服务...")
	// 这里以后可以加 graceful shutdown 逻辑，比如关数据库连接
	// sqlDB, _ := a.DB.DB(); sqlDB.Close()
	log.Println("👋 Bye!")

}

func (a *App) startWebSocketLisener() {
	headers := make(chan *types.Header)

	sub, err := a.EthClient.SubscribeNewHead(context.Background(), headers)
	if err != nil {
		log.Fatal("订阅失败:", err)
	}

	for {
		select {
		case err := <-sub.Err():
			log.Fatal("订阅异常中断:", err)

		case header := <-headers:
			go a.ProcessBlock(header)
		}

	}
}

func (a *App) ProcessBlock(header *types.Header) {
	block, err := a.EthClient.BlockByHash(context.Background(), header.Hash())
	if err != nil {
		log.Printf("❌ 获取区块体失败 #%s: %v", header.Number.String(), err)
		return
	}
	fmt.Printf("🧱 新区块: #%s | Hash: %s | Txs: %d\n",
		block.Number().String(), block.Hash().Hex(), len(block.Transactions()))

	newBlock := model.SQLBlock{
		Height:    block.NumberU64(),
		Hash:      block.Hash().Hex(),
		Timestamp: time.Unix(int64(block.Time()), 0),
		TxCount:   len(block.Transactions()),
		// 转换 BigInt 为 String 存库
		BaseFee: block.BaseFee().String(),
	}

	if err := a.BlockRepo.Save(&newBlock); err != nil {
		log.Printf("❌ 存库失败: %v", err)
	} else {
		log.Printf("💾 已存入 DB (ID: %d)", newBlock.ID)
	}

}
