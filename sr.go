package dht

import (
	"context"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-routing-language/syntax"
	"github.com/libp2p/go-smart-record/ir"
	"github.com/libp2p/go-smart-record/ir/base"
	"github.com/libp2p/go-smart-record/vm"
)

// This file includes the code for smart record operations

// PutSmartValue adds smart record value corresponding to given Key.
// This is the top level smart record "Store" operation of the DHT
// NOTE: We use a syntx.Dict as input, if we want to keep the signature of
// PutValue change value to []byte
func (dht *IpfsDHT) PutSmartValue(ctx context.Context, key string, value syntax.Dict, opts ...routing.Option) (err error) {
	// Check if smart-records enabled
	if dht.srClient == nil {
		return fmt.Errorf("smart records not supported")
	}

	logger.Debugw("putting smart value", "key", internal.LoggableRecordKeyString(key))

	// Get closest peers
	peers, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			// NOTE: We could add specific events for smart values
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.Value,
				ID:   p,
			})

			// Put smart value in every peer's vm.
			// NOTE: This is a sanity-check. We probably can remove it in the future.
			if p == dht.self {
				err := dht.srServer.UpdateLocal(key, p, value, dht.maxRecordAge)
				if err != nil {
					logger.Debugf("failed putting value to smart record locally: %s", err)
				}
			} else {
				err := dht.srClient.Update(ctx, key, p, value, dht.maxRecordAge)
				if err != nil {
					logger.Debugf("failed putting value to smart record: %s", err)
				}
			}

		}(p)
	}
	wg.Wait()

	return nil
}

// GetSmartValue searches for the smart record corresponding to given Key
// NOTE: We are returning a vm.recordValue right away.
func (dht *IpfsDHT) GetSmartValue(ctx context.Context, key string, opts ...routing.Option) (_ vm.RecordValue, err error) {

	if dht.srClient == nil {
		return nil, fmt.Errorf("smart records not supported")
	}

	/* NOTE: For now we don't use quorum. We let the lookup run till the end so
	/* we collect every smart records from peers in the path. We probably should
	/* change this in the future and allow users specify when to terminate the lookup
	// apply defaultQuorum if relevant
	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}
	opts = append(opts, Quorum(internalConfig.GetQuorum(&cfg)))
	*/

	// Finds closest peers and request smart records for peers in path.
	r, err := dht.SearchSmartValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	if ctx.Err() != nil {
		return r, ctx.Err()
	}
	if r == nil {
		return nil, routing.ErrNotFound
	}
	logger.Debugf("GetSmartValue %v %x", internal.LoggableRecordKeyString(key), r)
	return r, nil
}

func (dht *IpfsDHT) getSmartValues(ctx context.Context, key string, tmpVM vm.Machine, stopQuery chan struct{}) (<-chan *vm.RecordValue, <-chan *lookupWithFollowupResult) {
	lookupResCh := make(chan *lookupWithFollowupResult, 1)
	valCh := make(chan *vm.RecordValue, 1)
	logger.Debugw("finding smart value", "key", internal.LoggableRecordKeyString(key))

	// Look locally if we have any record for the key.
	// NOTE: We may not need this. Even we have local records we
	// want to gather the ones from closest peers.
	if rec := dht.srServer.GetLocal(key); rec != nil {
		select {
		case valCh <- &rec:
		case <-ctx.Done():
		}
	}

	go func() {
		defer close(valCh)
		defer close(lookupResCh)
		lookupRes, err := dht.runLookupWithFollowup(ctx, key,
			func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
				// For DHT query command
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type: routing.SendingQuery,
					ID:   p,
				})

				// Get smart record from remote peer
				t, err := dht.srClient.Get(ctx, key, p)
				if err != nil {
					// NOTE: Let's do nothing if Get fails for now.
					// We have other alternatives and we want to keep going.
					logger.Debugf("Error in get smartRecord", err)
					return nil, nil
				}
				select {
				case valCh <- t: // Send value for aggregation in VM
				case <-ctx.Done():
					return nil, ctx.Err()
				}

				return nil, nil
			},
			func() bool {
				select {
				case <-stopQuery: // TODO: stop query sends nothing. We run the lookup till th end
					return true
				default:
					return false
				}
			},
		)

		if err != nil {
			return
		}
		lookupResCh <- lookupRes

		if ctx.Err() == nil {
			dht.refreshRTIfNoShortcut(kb.ConvertKey(key), lookupRes)
		}
	}()

	return valCh, lookupResCh
}

// SearchValue searches for the value corresponding to given Key and streams the results.
func (dht *IpfsDHT) SearchSmartValue(ctx context.Context, key string, opts ...routing.Option) (vm.RecordValue, error) {

	/* NOTE: No routing options for now
	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}

	responsesNeeded := 0
	if !cfg.Offline {
	        responsesNeeded = internalConfig.GetQuorum(&cfg)
	}
	*/

	// Create temporary vm to aggregate records
	tmpVM, err := dht.tempVM()
	defer tmpVM.Close()
	if err != nil {
		return nil, fmt.Errorf("couldn't instantiate temp VM: %v", err)
	}

	stopCh := make(chan struct{})
	valCh, _ := dht.getSmartValues(ctx, key, tmpVM, stopCh)
	for v := range valCh {
		srOut := *v
		// Update with record. This function accounts for empty reecords
		err = updateRecord(tmpVM, key, srOut)
		if err != nil {
			// NOTE: Do nothing with this error for now.
		}

	}
	// Get from local vm.
	return tmpVM.Get(key), nil

	/* NOTE: No update of routing tables or quorum for now
	go func() {
		defer close(out)
		best, peersWithBest, aborted := dht.searchValueQuorum(ctx, key, valCh, stopCh, out, responsesNeeded)
		if best == nil || aborted {
			return
		}

		updatePeers := make([]peer.ID, 0, dht.bucketSize)
		select {
		case l := <-lookupRes:
			if l == nil {
				return
			}

			for _, p := range l.peers {
				if _, ok := peersWithBest[p]; !ok {
					updatePeers = append(updatePeers, p)
				}
			}
		case <-ctx.Done():
			return
		}

		dht.updatePeerValues(dht.Context(), key, best, updatePeers)
	}()
	return out, nil
	*/
}

func (dht *IpfsDHT) tempVM() (vm.Machine, error) {
	upCtx := ir.DefaultUpdateContext{}
	asmCtx := ir.AssemblerContext{Grammar: base.BaseGrammar}
	// Starting new temp VM to perform updates.
	return vm.NewVM(context.Background(), dht.host, upCtx, asmCtx)
}

func updateRecord(v vm.Machine, key string, with vm.RecordValue) error {
	for p, r := range with {
		// For each peer update the record
		err := v.Update(p, key, *r)
		if err != nil {
			return err
		}
	}
	return nil
}
