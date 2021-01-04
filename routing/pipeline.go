package routing

import (
	"context"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/peer"
)

var (
	logger = logging.Logger("dht.routing")
)

// RecvdVal stores a value and the peer from which we got the value.
type RecvdVal struct {
	Val  []byte
	From peer.ID
}

func RunRecordPipeline(ctx context.Context, processors []Processor, abortQuery func(), out chan<- []byte, in <-chan RecvdVal) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
	processInput:
		for {
			select {
			case i, more := <-in:
				if !more {
					return
				}
				var state interface{} = i
				var err error
				for _, t := range processors {
					state, err = t.Process(state, abortQuery)
					if err != nil {
						continue processInput

					}
				}

				finalState := state.(RecvdVal)
				select {
				case out <- finalState.Val:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}

			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()
	return errCh
}

func RunProvidersPipeline(ctx context.Context, processors []Processor, abortQuery func(), out chan<- peer.AddrInfo, in <-chan peer.AddrInfo) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
	processInput:
		for {
			select {
			case i, more := <-in:
				if !more {
					return
				}
				var state interface{} = i
				var err error
				for _, t := range processors {
					state, err = t.Process(state, abortQuery)
					if err != nil {
						continue processInput

					}
				}

				finalState := state.(peer.AddrInfo)
				select {
				case out <- finalState:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}

			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()
	return errCh
}
