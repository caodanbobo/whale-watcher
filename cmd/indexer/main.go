package main

import (
	"log"
	"whale-watcher/pkg/config"
)

// main initializes the indexer application and starts the event loop
func main() {
	// Load configuration from environment or config file
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("Configuration loading failed:", err)
	}

	// Initialize application with loaded configuration
	app, err := Initialize(cfg)
	if err != nil {
		log.Fatal("Application initialization failed:", err)
	}

	log.Println("🚀 Indexer service starting...")
	app.Run()
}
