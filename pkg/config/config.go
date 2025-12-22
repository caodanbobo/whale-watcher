package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DB_DSN     string
	EthWSURL   string
	AppPort    string
	FetchLimit int
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		DB_DSN:     getEnv("DB_DSN", ""),
		EthWSURL:   getEnv("ETH_WS_URL", ""),
		AppPort:    getEnv("APP_PORT", "8888"),
		FetchLimit: 10,
	}

	if cfg.DB_DSN == "" {
		return nil, log.Output(1, "❌ 错误: 环境变量 DB_DSN 未设置") // 简单的报错
	}
	if cfg.EthWSURL == "" {
		return nil, log.Output(1, "❌ 错误: 环境变量 ETH_WS_URL 未设置")
	}

	return cfg, nil
}

// getEnv 是一个辅助函数：读取环境变量，如果没读到就返回默认值
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
