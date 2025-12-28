package client

import (
	"context"
	"fmt"
	"time"
	"whale-watcher/pkg/model"
)

type MockClient struct{}

func NewMockClient() *MockClient {
	return &MockClient{}
}

func (m *MockClient) FetchBlock(ctx context.Context, height int) (*model.Block, error) {
	mockBlock := &model.Block{
		Number:    fmt.Sprintf("0x%x", height),
		Hash:      "0xMockHash...",
		Timestamp: "0x654321",
		Transactions: []model.Transaction{
			{Hash: "0xTx1...", Value: "0xDE0B6B3A7640000"},    // 1 ETH
			{Hash: "0xTx2...", Value: "0x3635C9ADC5DEA00000"}, // 1000 ETH (whale!)
		},
	}
	select {

	// Case A: Simulate network latency (triggered after 3 seconds)
	case <-time.After(500 * time.Millisecond):
		fmt.Printf("👻 [Mock] Pretend to fetch block #%d from network\n", height)
		return mockBlock, nil

	// Case B: Context timeout or cancellation (triggered at 2 seconds)
	case <-ctx.Done():
		// If timeout occurred, return error immediately, don't return Block
		return nil, ctx.Err() // returns "context deadline exceeded"
	}
}
