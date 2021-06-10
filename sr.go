package dht

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	u "github.com/ipfs/go-ipfs-util"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	internalConfig "github.com/libp2p/go-libp2p-kad-dht/internal/config"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-routing-language/syntax"
	"github.com/libp2p/go-smart-record/ir"
	"github.com/libp2p/go-smart-record/ir/base"
	"github.com/libp2p/go-smart-record/vm"
)

// Value stored in a key of the DHT to let others know that this value
// is stored as a Smart Record
var SRPointerValue = []byte("sr")

// This file includes the code for smart record operations

// PutSmartValue adds smart record value corresponding to given Key.
// This is the top level smart record "Store" operation of the DHT
// NOTE: We are inputting a Dict, if we want to keep the signature of
// PutValue change value to []byte
func (dht *IpfsDHT) PutSmartValue(ctx context.Context, key string, value syntax.Dict, opts ...routing.Option) (err error) {
	if !dht.enableValues {
		return routing.ErrNotSupported
	}

	if dht.srClient == nil {
		return fmt.Errorf("smart records not supported")
	}

	logger.Debugw("putting smart value", "key", internal.LoggableRecordKeyString(key))

	// don't even allow local users to put bad values.
	if err := dht.Validator.Validate(key, SRPointerValue); err != nil {
		return err
	}

	old, err := dht.getLocal(key)
	if err != nil {
		// Means something is wrong with the datastore.
		return err
	}

	// Check if we have an old value that's not the same as the new one.
	if old != nil && !bytes.Equal(old.GetValue(), SRPointerValue) {
		// Check to see if the new one is better.
		i, err := dht.Validator.Select(key, [][]byte{SRPointerValue, old.GetValue()})
		if err != nil {
			return err
		}
		if i != 0 {
			return fmt.Errorf("can't replace a newer value with an older value")
		}
	}

	rec := record.MakePutRecord(key, SRPointerValue)
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dht.putLocal(key, rec)
	if err != nil {
		return err
	}
	peers, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	// Marshal value into syntax.Dict
	// TODO: This will be marshalled again in srClient.Update.
	// Consider either making a syntax.Dict as the input for PutSmartValue,
	// or changing srClient to accept bytes directly.
	// (I am more fond of the first first one)
	// n, err := syntax.UnmarshalJSON(value)
	// d, ok := n.(syntax.Dict)
	// if !ok {
	// 	return fmt.Errorf("smart record value is not syntax.Dict")
	// }

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.Value,
				ID:   p,
			})

			// Put smart value in every peer's vm.
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
				// Put pointer to SR in the DHT
				// NOTE: Instead of putting this pointer to point to smart records,
				// we could add a PUT_SMART_VALUE and GET_SMART_VALUE operation in
				// in the DHT protocol to signal when to check DHT datastore and SR VM.
				// This would lead to having different handlers for PUT_VALUE and PUT_SMART_VALUE
				err = dht.protoMessenger.PutValue(ctx, p, rec)
				if err != nil {
					logger.Debugf("failed putting sr pointer to peer: %s", err)
				}
			}

		}(p)
	}
	wg.Wait()

	return nil
}

// GetSmartValue searches for the smart record corresponding to given Key
// NOTE: We are returning a recordValue.
func (dht *IpfsDHT) GetSmartValue(ctx context.Context, key string, opts ...routing.Option) (_ vm.RecordValue, err error) {
	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	if dht.srClient == nil {
		return nil, fmt.Errorf("smart records not supported")
	}

	// apply defaultQuorum if relevant
	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}
	opts = append(opts, Quorum(internalConfig.GetQuorum(&cfg)))

	responses, err := dht.SearchSmartValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	// Create temporary vm
	tmpVM, err := localVM()
	if err != nil {
		return nil, fmt.Errorf("Couldn't instantiate local VM: %v", err)
	}

	// For each smart value received update with the current one
	// to get final state
	for r := range responses {
		// Check if the value is a pointer to smart record
		if string(r.Val) == string(SRPointerValue) {
			// Get smart record from peer
			// TODO: Check locally if r.From is self. It means that I have it.
			var srOut vm.RecordValue
			if r.From == dht.self {
				srOut = dht.srServer.GetLocal(key)
			} else {
				t, err := dht.srClient.Get(ctx, key, r.From)
				srOut = *t
				if err != nil {
					// NOTE: Let's do nothing if Get fails for now.
					// We have other alternatives
					fmt.Println(err)
					continue
				}
			}
			// Update with record
			err = updateRecord(tmpVM, key, srOut)
			if err != nil {
				// NOTE: Let's do nothing if Get fails for now.
				// We have other alternatives
				continue
			}
		}
	}

	// Get final update from smart record and return it
	best := tmpVM.Get(key)

	if ctx.Err() != nil {
		return best, ctx.Err()
	}

	if best == nil {
		return nil, routing.ErrNotFound
	}
	logger.Debugf("GetValue %v %x", internal.LoggableRecordKeyString(key), best)
	return best, nil
}

// SearchValue searches for the value corresponding to given Key and streams the results.
func (dht *IpfsDHT) SearchSmartValue(ctx context.Context, key string, opts ...routing.Option) (<-chan RecvdVal, error) {
	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}

	responsesNeeded := 0
	if !cfg.Offline {
		responsesNeeded = internalConfig.GetQuorum(&cfg)
	}

	stopCh := make(chan struct{})
	valCh, lookupRes := dht.getValues(ctx, key, stopCh)

	out := make(chan RecvdVal)
	go func() {
		defer close(out)
		best, peersWithBest, aborted := dht.searchSmartValueQuorum(ctx, key, valCh, stopCh, out, responsesNeeded)
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
}

func (dht *IpfsDHT) searchSmartValueQuorum(ctx context.Context, key string, valCh <-chan RecvdVal, stopCh chan struct{},
	out chan<- RecvdVal, nvals int) ([]byte, map[peer.ID]struct{}, bool) {
	numResponses := 0
	return dht.processValues(ctx, key, valCh,
		func(ctx context.Context, v RecvdVal, better bool) bool {
			numResponses++
			if better {
				select {
				case out <- v:
				case <-ctx.Done():
					return false
				}
			}

			if nvals > 0 && numResponses > nvals {
				close(stopCh)
				return true
			}
			return false
		})
}

func localVM() (vm.Machine, error) {
	upCtx := ir.DefaultUpdateContext{}
	asmCtx := ir.AssemblerContext{Grammar: base.BaseGrammar}
	// Starting new temp VM to perform updates.
	return vm.NewVM(context.Background(), upCtx, asmCtx)
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
