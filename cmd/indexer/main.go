package main

import (
	"log"
	"whale-watcher/pkg/config"
)

func main() {
	cfg, err := config.Load()

	if err != nil {
		log.Fatal("配置加载失败")
	}
	app, err := Initialize(cfg)
	if err != nil {
		log.Fatal("应用初始化失败:", err)
	}
	log.Println("🚀 Indexer 服务启动中...")
	app.Run()
}
