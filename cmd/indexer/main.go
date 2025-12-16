package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"
	"whale-watcher/pkg/model"
	"whale-watcher/pkg/repository"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const WorkerCount = 5

func IsWhaleTransfer(wei *big.Int) (bool, string) {
	threshold := new(big.Int)
	//10^20 100 ether
	threshold.Exp(big.NewInt(10), big.NewInt(20), nil)

	isWhale := wei.Cmp(threshold) >= 0
	//1 ether
	ethDivisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	ethValue := new(big.Int).Div(wei, ethDivisor)

	return isWhale, ethValue.String()
}

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Println("⚠️ 未发现 .env 文件，将尝试使用系统环境变量")
	}

	dsn := os.Getenv("DB_DSN")
	wsURL := os.Getenv("ETH_WS_URL")

	db, err := initDB(dsn)

	if err != nil {
		log.Fatal("数据库连接失败:", err)
	}
	fmt.Println("✅ 数据库连接成功")

	blockRepo := repository.NewBlockRepo(db)

	client, err := ethclient.Dial(wsURL)

	if err != nil {
		log.Fatal("WebSocket 连接失败:", err)
	}
	fmt.Println("✅ WebSocket 连接成功，开始监听...")

	headers := make(chan *types.Header)

	sub, err := client.SubscribeNewHead(context.Background(), headers)
	if err != nil {
		log.Fatal("订阅失败:", err)
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case err := <-sub.Err():
			log.Fatal("订阅异常中断:", err)

		case header := <-headers:
			go ProcessBlock(client, blockRepo, header)

		case <-sigs:
			fmt.Println("🛑 停止监听")
			return
		}

	}
}

func initDB(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	db.AutoMigrate(&model.SQLBlock{})
	return db, nil
}

func ProcessBlock(client *ethclient.Client, repo repository.BlockRepository, header *types.Header) {
	block, err := client.BlockByHash(context.Background(), header.Hash())
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

	if err := repo.Save(&newBlock); err != nil {
		log.Printf("❌ 存库失败: %v", err)
	} else {
		log.Printf("💾 已存入 DB (ID: %d)", newBlock.ID)
	}
}
