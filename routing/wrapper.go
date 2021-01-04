package routing

import (
	"context"
	"github.com/multiformats/go-multihash"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
)

type getValuesFn func(ctx, queryAbortedCtx context.Context, key string, seedPeerOpts SeedPeersOptions) (<-chan RecvdVal, <-chan []peer.ID)
type updatePeerValuesFn func(ctx context.Context, best []byte, closestPeers []peer.ID)

// SearchValue searches for the value corresponding to given Key and streams the results.
func SearchValue(ctx context.Context, key string, getVals getValuesFn, processors []Processor, updatePeerValues updatePeerValuesFn, cfg *routing.Options) (<-chan []byte, <-chan []peer.ID, error) {
	seedPeerOpts := GetSeedPeers(cfg)
	updateDuringGet := getUpdateDuringGet(cfg, true)
	queryAbortCtx, abortQuery := context.WithCancel(context.Background())
	valCh, closestPeersCh := getVals(ctx, queryAbortCtx, key, seedPeerOpts)

	out := make(chan []byte)
	returnClosestPeersCh := make(chan []peer.ID, 1)

	var closestPeers []peer.ID

	go func() {
		defer close(out)
		defer close(returnClosestPeersCh)
		defer abortQuery()
		errCh := RunRecordPipeline(ctx, processors, abortQuery, out, valCh)
		if err := <-errCh; err != nil {
			return
		}

		closestPeers = <-closestPeersCh
		if updateDuringGet && updatePeerValues != nil {
			for _, f := range processors {
				if bv, ok := f.(*BestValueFilterRecorder); ok {
					if bv.Best == nil {
						break
					}

					updatePeers := make([]peer.ID, 0, len(closestPeers))
					for _, p := range closestPeers {
						if _, ok := bv.PeersWithBest[p]; !ok {
							updatePeers = append(updatePeers, p)
						}
					}
					updatePeerValues(ctx, bv.Best, updatePeers)
					break
				}
			}
		}
		returnClosestPeersCh <- closestPeers
	}()

	return out, returnClosestPeersCh, nil
}

type findProvsFn func(ctx, queryAbortedCtx context.Context, key multihash.Multihash, seedPeerOpts SeedPeersOptions) (<-chan peer.AddrInfo, <-chan []peer.ID)

// FindProviders searches for the providers corresponding to given Key and streams the results.
func FindProviders(ctx context.Context, key multihash.Multihash, findProvsFn findProvsFn, processors []Processor, cfg *routing.Options) (<-chan peer.AddrInfo, <-chan []peer.ID, error) {
	seedPeerOpts := GetSeedPeers(cfg)
	maxRequestedRecords := GetQuorum(cfg)

	queryAbortCtx, abortQuery := context.WithCancel(context.Background())
	valCh, closestPeersCh := findProvsFn(ctx, queryAbortCtx, key, seedPeerOpts)

	outChSize := maxRequestedRecords
	if outChSize == 0 {
		outChSize = 1
	}
	out := make(chan peer.AddrInfo, outChSize)
	returnClosestPeersCh := make(chan []peer.ID, 1)

	go func() {
		defer close(out)
		defer close(returnClosestPeersCh)
		defer abortQuery()
		errCh := RunProvidersPipeline(ctx, processors, abortQuery, out, valCh)
		if err := <-errCh; err != nil {
			return
		}

		closestPeers := <-closestPeersCh
		returnClosestPeersCh <- closestPeers
	}()

	return out, returnClosestPeersCh, nil
}
