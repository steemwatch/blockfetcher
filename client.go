package blockfetcher

import (
	"errors"
	"time"

	"github.com/go-steem/rpc"
	"github.com/pkg/errors"
)

var (
	errInterrupted = errors.New("interrupted")
	errTerminating = errors.New("terminating")
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
	addr    string
	timeout time.Duration

	c *rpc.Client

	termCh    chan struct{}
	termAckCh chan struct{}
}

func newReconnectingClient(addr string, timeout time.Duration) *reconnectingClient {
	return &reconnectingClient{
		addr:      addr,
		timeout:   timeout,
		termCh:    make(chan struct{}),
		termAckCh: make(chan struct{}),
	}
}

func (client *reconnectingClient) acquireClient() (*rpc.Client, error) {
	// In case there is a connected client, return it.
	if c := client.c; c != nil {
		return c, nil
	}

	// Otherwise connect to the RPC endpoint and return the client.
	for {
		var (
			c      *rpc.Client
			err    error
			doneCh = make(chan struct{})
		)
		go func() {
			c, err = rpc.Dial(client.addr)
			close(doneCh)
		}()

		select {
		case <-doneCh:
		case <-client.termCh:
			return nil, ErrInterrupted
		}

		if _, ok := err.(*websocker.DialError); ok {
			select {
			case <-time.After(1 * time.Minute):
				continue
			case <-client.termCh:
				return nil, ErrInterrupted
			}
		}
	}
}

// do is never running multiple times in parallel.
func (client *reconnectingClient) do(func() (interface{}, error)) (interface{}, error) {
	for {
		c, err := client.newClient()
		if err != nil {
			return nil, err
		}

		var (
			config *rpc.Config
			err    error
			doneCh = make(chan struct{})
		)
		go func() {
			config, err = c.GetConfig()
			close(doneCh)
		}()

		select {
		case <-doneCh:
		case <-time.After(client.timeout):
			client.closeClient()
		}

		if err != nil {
			if _, ok := err.(*net.OpError); ok {
				client.closeClient()
				continue
			}

			return nil, err
		}

		return config, nil
	}
}

func (client *reconnectingClient) GetDynamicGlobalProperties() (*rpc.DynamicGlobalProperties, error) {
	v, err := client.do(func(c *rpc.Client) (interface{}, error) {
		return c.GetDynamicGlobalProperties()
	})
	if err != nil {
		return err
	}
	return v.(*rpc.DynamicGlobalProperties), nil
}

func (client *reconnectingClient) GetBlock(blockNum uint32) (*rpc.Block, error) {
	v, err := client.do(func(c *rpc.Client) (interface{}, error) {
		return c.GetBlock(blockNum)
	})
	if err != nil {
		return err
	}
	return v.(*rpc.Block), nil
}

func (client *reconnectingClient) Close() error {
	select {
	case <-client.termCh:
		return ErrTerminating
	default:
		close(client.termCh)
	}
	<-client.termAckCh
}
