package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
	"whale-watcher/pkg/model"
)

const (
	rpcMethodGetBlock = "eth_getBlockByNumber"
	rpcVersion        = "2.0"
	rpcID             = 1
	httpContentType   = "application/json"
	defaultHTTPTimeout = 10 * time.Second
)

// EthClient provides methods to interact with Ethereum JSON-RPC endpoints
type EthClient struct {
	url    string
	client *http.Client
}

// NewEthClient creates a new Ethereum RPC client with default timeout
func NewEthClient(url string) *EthClient {
	return &EthClient{
		url: url,
		client: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
	}
}

// FetchBlock retrieves a block by its height from the Ethereum network
func (c *EthClient) FetchBlock(ctx context.Context, height int) (*model.Block, error) {
	hexHeight := fmt.Sprintf("0x%x", height)

	rpcRequest := c.createBlockRequest(hexHeight)
	rpcBlock, err := c.sendRPCRequest(ctx, rpcRequest)
	if err != nil {
		return nil, err
	}

	return rpcBlock, nil
}

// createBlockRequest builds an RPC request for eth_getBlockByNumber
func (c *EthClient) createBlockRequest(hexHeight string) model.RPCRequest {
	return model.RPCRequest{
		JsonRPC: rpcVersion,
		Method:  rpcMethodGetBlock,
		Params:  []interface{}{hexHeight, true},
		ID:      rpcID,
	}
}

// sendRPCRequest sends a JSON-RPC request and parses the response
func (c *EthClient) sendRPCRequest(ctx context.Context, rpcRequest model.RPCRequest) (*model.Block, error) {
	// Marshal request to JSON
	jsonData, err := json.Marshal(rpcRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal RPC request: %w", err)
	}

	// Create HTTP request with context
	req, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", httpContentType)

	// Send HTTP request
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Unmarshal RPC response
	var rpcResponse model.RPCResponse
	if err := json.Unmarshal(body, &rpcResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RPC response: %w", err)
	}

