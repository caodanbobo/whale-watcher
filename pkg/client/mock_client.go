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
			{Hash: "0xTx2...", Value: "0x3635C9ADC5DEA00000"}, // 1000 ETH (巨鲸!)
		},
	}
	select {

	// 情况 A: 模拟网络耗时 (3秒后触发)
	case <-time.After(500 * time.Millisecond):
		fmt.Printf("👻 [Mock] 假装从网络获取了区块 #%d\n", height)
		return mockBlock, nil

	// 情况 B: Context 超时或被取消 (2秒时就会触发)
	case <-ctx.Done():
		// 既然超时了，就立刻返回错误，不要再返回 Block 了
		return nil, ctx.Err() // 返回 "context deadline exceeded"
	}
}
