// Package blockfetcher aims to reduce boilerplate code that one needs to write
// over and over again when implementing a program that is processing blocks
// that are being fetched over steemd WebSocket RPC endpoint.
//
// All you need from now in is to import this package and implement BlockProcessor interface,
// then run the block fetcher with your custom BlockProcessor implementation:
//
//     ctx, err := blockfetcher.Run(blockProcessor)
//
// You can wait for the fetcher to be done by calling
//
//     err := ctx.Wait()
//
// In case you want to interrupt the process, just call
//
//     ctx.Interrupt()
//     err := ctx.Wait()
package blockfetcher

import (
	"time"

	"github.com/go-steem/rpc"
	"github.com/go-steem/rpc/apis/database"
	"github.com/pkg/errors"
	"gopkg.in/tomb.v2"
)

// BlockProcessor is the interface that represents the block processing logic.
//
// When an error is returned from any for the following methods,
// the fetching process is interrupted and Finalize() is called.
type BlockProcessor interface {
	// BlockRange is called at the beginning to let the block fetching logic know
	// what blocks to fetch and pass to ProcessBlock.
	//
	// In case blockRangeTo returned is 0, the fetcher will keep fetching new blocks
	// forever as they arrive (until interrupted, of course).
	BlockRange() (blockRangeFrom, blockRangeTo uint32)

	// ProcessBlock is called when a new block is received.
	ProcessBlock(block *database.Block) error

	// Finalize is called when the whole block range is fetcher or the process is interrupted.
	Finalize() error
}

// Context represents a running block fetcher that can be interrupted.
type Context struct {
	client *rpc.Client

	processor BlockProcessor

	blockCh chan *database.Block

	t tomb.Tomb
}

// Run spawns a new block fetcher using the given BlockProcessor.
//
// The fetcher keeps fetching blocks until the whole block range specified is fetched
// or an error is encountered. It is not trying to be clever about closed connections and such.
//
// client.Close() is not called by this package, it has to be called manually.
func Run(client *rpc.Client, processor BlockProcessor) (*Context, error) {
	// Prepare a new Context object.
	ctx := &Context{
		client:    client,
		processor: processor,
		blockCh:   make(chan *database.Block),
	}

	// Start the fetcher and the finalizer.
	ctx.t.Go(ctx.fetcher)
	ctx.t.Go(ctx.finalizer)

	// Return the new context.
	return ctx, nil
}

// Interrupt interrupts the block fetcher and stops the fetching process.
func (ctx *Context) Interrupt() {
	ctx.t.Kill(nil)
}

// Wait blocks until the fetcher is stopped and returns any error encountered.
func (ctx *Context) Wait() error {
	return ctx.t.Wait()
}

func (ctx *Context) fetcher() error {
	// Get the block range to process.
	from, to := ctx.processor.BlockRange()

	// Decide whether to fetch a closed range or watch
	// and enter the right loop accordingly.
	var err error
	if to == 0 {
		err = ctx.blockWatcher(from)
	} else {
		err = ctx.blockFetcher(from, to)
	}
	return err
}

func (ctx *Context) finalizer() error {
	// Wait for the dying signal.
	<-ctx.t.Dying()

	// Run the finalizer.
	if err := ctx.processor.Finalize(); err != nil {
		return errors.Wrap(err, "BlockProcessor.Finalize() failed")
	}
	return nil
}

func (ctx *Context) blockWatcher(from uint32) error {
	next := from

	// Get config.
	config, err := ctx.client.Database.GetConfig()
	if err != nil {
		return errors.Wrap(err, "failed to get steemd config")
	}

	// Fetch and process all blocks matching the given range.
	for {
		// Get current properties.
		props, err := ctx.client.Database.GetDynamicGlobalProperties()
		if err != nil {
			return errors.Wrap(err, "failed to get steemd dynamic global properties")
		}

		// Process new blocks.
		for ; props.LastIrreversibleBlockNum >= next; next++ {
			if err := ctx.fetchAndProcess(next); err != nil {
				return err
			}
		}

		// Wait for STEEMIT_BLOCK_INTERVAL seconds before the next iteration.
		// In case Interrupt() is called, we exit immediately.
		select {
		case <-time.After(time.Duration(config.SteemitBlockInterval) * time.Second):
		case <-ctx.t.Dying():
			return nil
		}
	}
}

func (ctx *Context) blockFetcher(from, to uint32) error {
	next := from

	// Make sure we are not doing bullshit.
	if from > to {
		return errors.Errorf("invalid block range: [%v, %v]", from, to)
	}

	// Fetch and process all blocks matching the given range.
	for ; next <= to; next++ {
		if err := ctx.fetchAndProcess(next); err != nil {
			return err
		}
	}

	// The whole range has been processed, we are done.
	return nil
}

func (ctx *Context) fetchAndProcess(blockNum uint32) (err error) {
	defer handlePanic(&err)

	// Check for the dying signal first.
	select {
	case <-ctx.t.Dying():
		return tomb.ErrDying
	default:
	}

	// Fetch the block.
	block, err := ctx.client.Database.GetBlock(blockNum)
	if err != nil {
		return errors.Wrapf(err, "failed to fetch block %v", blockNum)
	}

	// Process the block.
	if err := ctx.processor.ProcessBlock(block); err != nil {
		return errors.Wrapf(err, "BlockProcessor.ProcessBlock() failed for block %v", blockNum)
	}
	return nil
}

func handlePanic(errPtr *error) {
	if r := recover(); r != nil {
		switch r := r.(type) {
		case error:
			*errPtr = errors.Wrap(r, "panic recovered")
		case string:
			*errPtr = errors.New(r)
		default:
			panic(r)
		}
	}
}
