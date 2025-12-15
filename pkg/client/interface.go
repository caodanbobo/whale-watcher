package client

import (
	"context"
	"whale-watcher/pkg/model"
)

type ChainClient interface {
	FetchBlock(ctx context.Context, height int) (*model.Block, error)
}
