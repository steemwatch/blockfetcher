package blockfetcher

import (
	"github.com/go-steem/rpc"
)

// client interface incorporates the parts of *rpc.Client
// that are needed by the block fetcher.
type client interface {
	GetConfig() (*rpc.Config, error)
	GetDynamicGlobalProperties() (*rpc.DynamicGlobalProperties, error)
	GetBlock(blockNum uint32) (*rpc.Block, error)
	Close() error
}

type reconnectingClient struct {
	addr string
}

func (client *reconnectingClient) GetConfig() (*rpc.Config, error) {

}

func (client *reconnectingClient) GetDynamicGlobalProperties() (*rpc.DynamicGlobalProperties, error) {

}

func (client *reconnectingClient) GetBlock(blockNum uint32) (*rpc.Block, error) {

}

func (client *reconnectingClient) Close() error {

}
