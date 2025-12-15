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

type EthClient struct {
	url    string
	client *http.Client
}

func NewEthClient(url string) *EthClient {
	return &EthClient{
		url: url,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *EthClient) FetchBlock(ctx context.Context, height int) (*model.Block, error) {
	hexHeight := fmt.Sprintf("0x%x", height)

	requestBody := model.RPCRequest{
		JsonRPC: "2.0",
		Method:  "eth_getBlockByNumber",
		Params:  []interface{}{hexHeight, true},
		ID:      1,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		log.Fatal("JSON 初始化失败", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatal("创建请求失败:", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		log.Fatal("发送请求失败:", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("读取响应失败:", err)
	}

	var rpcResponse model.RPCResponse
	if err := json.Unmarshal(body, &rpcResponse); err != nil {
		return nil, err
	}
	return &rpcResponse.Result, nil
}
