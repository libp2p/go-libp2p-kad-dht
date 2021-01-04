package dht

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/routing"

	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	dhtrouting "github.com/libp2p/go-libp2p-kad-dht/routing"
	kb "github.com/libp2p/go-libp2p-kbucket"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/multiformats/go-multihash"
)

// This file implements the Routing interface for the IpfsDHT struct.

// Basic Put/Get

// PutValueExtended adds value corresponding to given Key.
func (dht *IpfsDHT) PutValueExtended(ctx context.Context, key string, value []byte, opts ...routing.Option) ([]peer.ID, error) {
	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	logger.Debugw("putting value", "key", internal.LoggableRecordKeyString(key))

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}
	seedPeerOpts := dhtrouting.GetSeedPeers(&cfg)

	// don't even allow local users to put bad values.
	if err := dht.Validator.Validate(key, value); err != nil {
		return nil, err
	}

	old, err := dht.getLocal(key)
	if err != nil {
		// Means something is wrong with the datastore.
		return nil, err
	}

	// Check if we have an old value that's not the same as the new one.
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		// Check to see if the new one is better.
		i, err := dht.Validator.Select(key, [][]byte{value, old.GetValue()})
		if err != nil {
			return nil, err
		}
		if i != 0 {
			return nil, fmt.Errorf("can't replace a newer value with an older value")
		}
	}

	rec := record.MakePutRecord(key, value)
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dht.putLocal(key, rec)
	if err != nil {
		return nil, err
	}

	pchan, err := dht.GetClosestPeersSeeded(ctx, key, seedPeerOpts.SeedPeers, seedPeerOpts.UseRTPeers)
	if err != nil {
		return nil, err
	}

	closestPeers := make([]peer.ID, 0, dht.bucketSize)
	wg := sync.WaitGroup{}
	for p := range pchan {
		closestPeers = append(closestPeers, p)
		wg.Add(1)
		go func(p peer.ID) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.Value,
				ID:   p,
			})

			err := dht.protoMessenger.PutValue(ctx, p, rec)
			if err != nil {
				logger.Debugf("failed putting value to peer: %s", err)
			}
		}(p)
	}
	wg.Wait()

	return closestPeers, nil
}

// PutValue adds value corresponding to given Key.
// This is the top level "Store" operation of the DHT

func (dht *IpfsDHT) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) (err error) {
	_, err = dht.PutValueExtended(ctx, key, value, opts...)
	return err
}

// RecvdVal stores a value and the peer from which we got the value.
type RecvdVal struct {
	Val  []byte
	From peer.ID
}

// GetValue searches for the value corresponding to given Key.
func (dht *IpfsDHT) GetValue(ctx context.Context, key string, opts ...routing.Option) (_ []byte, err error) {
	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	responses, err := dht.SearchValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	var best []byte

	for r := range responses {
		best = r
	}

	if ctx.Err() != nil {
		return best, ctx.Err()
	}

	if best == nil {
		return nil, routing.ErrNotFound
	}
	logger.Debugf("GetValue %v %x", internal.LoggableRecordKeyString(key), best)
	return best, nil
}

// SearchValueExtended searches for the value corresponding to given Key and streams the results.
func (dht *IpfsDHT) SearchValueExtended(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, <-chan []peer.ID, error) {
	if !dht.enableValues {
		return nil, nil, routing.ErrNotSupported
	}

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, nil, err
	}

	responsesNeeded := 0
	if !cfg.Offline {
		responsesNeeded = dhtrouting.GetQuorum(&cfg)
	}

	seedPeerOpts := dhtrouting.GetSeedPeers(&cfg)

	stopCh := make(chan struct{})
	valCh, lookupRes := dht.getValues(ctx, key, seedPeerOpts, stopCh)

	out := make(chan []byte)
	peers := make(chan []peer.ID, 1)
	go func() {
		defer close(out)
		defer close(peers)
		best, peersWithBest, aborted := dht.searchValueQuorum(ctx, key, valCh, stopCh, out, responsesNeeded)

		var l *lookupWithFollowupResult
		select {
		case l = <-lookupRes:
		case <-ctx.Done():
			return
		}

		if l == nil {
			return
		}

		if best == nil || aborted {
			peers <- l.peers
			return
		}

		updatePeers := make([]peer.ID, 0, dht.bucketSize)
		for _, p := range l.peers {
			if _, ok := peersWithBest[p]; !ok {
				updatePeers = append(updatePeers, p)
			}
		}

		dht.updatePeerValues(dht.Context(), key, best, updatePeers)
	}()

	return out, peers, nil
}

// SearchValue searches for the value corresponding to given Key and streams the results.
func (dht *IpfsDHT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	out, _, err := dht.SearchValueExtended(ctx, key, opts...)
	return out, err
}

func (dht *IpfsDHT) searchValueQuorum(ctx context.Context, key string, valCh <-chan RecvdVal, stopCh chan struct{},
	out chan<- []byte, nvals int) ([]byte, map[peer.ID]struct{}, bool) {
	numResponses := 0
	return dht.processValues(ctx, key, valCh,
		func(ctx context.Context, v RecvdVal, better bool) bool {
			numResponses++
			if better {
				select {
				case out <- v.Val:
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

// GetValues gets nvals values corresponding to the given key.
func (dht *IpfsDHT) GetValues(ctx context.Context, key string, nvals int) (_ []RecvdVal, err error) {
	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	valCh, _ := dht.getValues(queryCtx, key, dhtrouting.SeedPeersOptions{UseRTPeers: true}, nil)

	out := make([]RecvdVal, 0, nvals)
	for val := range valCh {
		out = append(out, val)
		if len(out) == nvals {
			cancel()
		}
	}

	return out, ctx.Err()
}

func (dht *IpfsDHT) processValues(ctx context.Context, key string, vals <-chan RecvdVal,
	newVal func(ctx context.Context, v RecvdVal, better bool) bool) (best []byte, peersWithBest map[peer.ID]struct{}, aborted bool) {
loop:
	for {
		if aborted {
			return
		}

		select {
		case v, ok := <-vals:
			if !ok {
				break loop
			}

			// Select best value
			if best != nil {
				if bytes.Equal(best, v.Val) {
					peersWithBest[v.From] = struct{}{}
					aborted = newVal(ctx, v, false)
					continue
				}
				sel, err := dht.Validator.Select(key, [][]byte{best, v.Val})
				if err != nil {
					logger.Warnw("failed to select best value", "key", internal.LoggableRecordKeyString(key), "error", err)
					continue
				}
				if sel != 1 {
					aborted = newVal(ctx, v, false)
					continue
				}
			}
			peersWithBest = make(map[peer.ID]struct{})
			peersWithBest[v.From] = struct{}{}
			best = v.Val
			aborted = newVal(ctx, v, true)
		case <-ctx.Done():
			return
		}
	}

	return
}

func (dht *IpfsDHT) updatePeerValues(ctx context.Context, key string, val []byte, peers []peer.ID) {
	fixupRec := record.MakePutRecord(key, val)
	for _, p := range peers {
		go func(p peer.ID) {
			//TODO: Is this possible?
			if p == dht.self {
				err := dht.putLocal(key, fixupRec)
				if err != nil {
					logger.Error("Error correcting local dht entry:", err)
				}
				return
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()
			err := dht.protoMessenger.PutValue(ctx, p, fixupRec)
			if err != nil {
				logger.Debug("Error correcting DHT entry: ", err)
			}
		}(p)
	}
}

func (dht *IpfsDHT) getValues(ctx context.Context, key string, seedPeerOpts dhtrouting.SeedPeersOptions, stopQuery chan struct{}) (<-chan RecvdVal, <-chan *lookupWithFollowupResult) {
	valCh := make(chan RecvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)

	logger.Debugw("finding value", "key", internal.LoggableRecordKeyString(key))

	if rec, err := dht.getLocal(key); rec != nil && err == nil {
		select {
		case valCh <- RecvdVal{
			Val:  rec.GetValue(),
			From: dht.self,
		}:
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

				rec, peers, err := dht.protoMessenger.GetValue(ctx, p, key)
				switch err {
				case routing.ErrNotFound:
					// in this case, they responded with nothing,
					// still send a notification so listeners can know the
					// request has completed 'successfully'
					routing.PublishQueryEvent(ctx, &routing.QueryEvent{
						Type: routing.PeerResponse,
						ID:   p,
					})
					return nil, err
				default:
					return nil, err
				case nil, internal.ErrInvalidRecord:
					// in either of these cases, we want to keep going
				}

				// TODO: What should happen if the record is invalid?
				// Pre-existing code counted it towards the quorum, but should it?
				if rec != nil && rec.GetValue() != nil {
					rv := RecvdVal{
						Val:  rec.GetValue(),
						From: p,
					}

					select {
					case valCh <- rv:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}

				// For DHT query command
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type:      routing.PeerResponse,
					ID:        p,
					Responses: peers,
				})

				return peers, err
			},
			func() bool {
				select {
				case <-stopQuery:
					return true
				default:
					return false
				}
			},
			seedPeerOpts.SeedPeers, seedPeerOpts.UseRTPeers,
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

func (dht *IpfsDHT) refreshRTIfNoShortcut(key kb.ID, lookupRes *lookupWithFollowupResult) {
	if lookupRes.completed {
		// refresh the cpl for this key as the query was successful
		dht.routingTable.ResetCplRefreshedAtForID(key, time.Now())
	}
}

// Provider abstraction for indirect stores.
// Some DHTs store values directly, while an indirect store stores pointers to
// locations of the value, similarly to Coral and Mainline DHT.

// ProvideExtended makes this node announce that it can provide a value for the given key
func (dht *IpfsDHT) ProvideExtended(ctx context.Context, key cid.Cid, brdcst bool, opts ...routing.Option) ([]peer.ID, error) {
	if !dht.enableProviders {
		return nil, routing.ErrNotSupported
	} else if !key.Defined() {
		return nil, fmt.Errorf("invalid cid: undefined")
	}
	keyMH := key.Hash()
	logger.Debugw("providing", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}
	seedPeerOpts := dhtrouting.GetSeedPeers(&cfg)

	// add self locally
	dht.ProviderManager.AddProvider(ctx, keyMH, dht.self)
	if !brdcst {
		return nil, nil
	}

	closerCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		now := time.Now()
		timeout := deadline.Sub(now)

		if timeout < 0 {
			// timed out
			return nil, context.DeadlineExceeded
		} else if timeout < 10*time.Second {
			// Reserve 10% for the final put.
			deadline = deadline.Add(-timeout / 10)
		} else {
			// Otherwise, reserve a second (we'll already be
			// connected so this should be fast).
			deadline = deadline.Add(-time.Second)
		}
		var cancel context.CancelFunc
		closerCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	var exceededDeadline bool
	peers, err := dht.GetClosestPeersSeeded(closerCtx, string(keyMH), seedPeerOpts.SeedPeers, seedPeerOpts.UseRTPeers)
	switch err {
	case context.DeadlineExceeded:
		// If the _inner_ deadline has been exceeded but the _outer_
		// context is still fine, provide the value to the closest peers
		// we managed to find, even if they're not the _actual_ closest peers.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		exceededDeadline = true
	case nil:
	default:
		return nil, err
	}

	closestPeers := make([]peer.ID, 0, dht.bucketSize)
	wg := sync.WaitGroup{}
	for p := range peers {
		closestPeers = append(closestPeers, p)
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			logger.Debugf("putProvider(%s, %s)", internal.LoggableProviderRecordBytes(keyMH), p)
			err := dht.protoMessenger.PutProvider(ctx, p, keyMH, dht.host)
			if err != nil {
				logger.Debug(err)
			}
		}(p)
	}
	wg.Wait()
	if exceededDeadline {
		return nil, context.DeadlineExceeded
	}
	return closestPeers, ctx.Err()
}

// Provide makes this node announce that it can provide a value for the given key
func (dht *IpfsDHT) Provide(ctx context.Context, key cid.Cid, brdcst bool) (err error) {
	_, err = dht.ProvideExtended(ctx, key, brdcst)
	return err
}

// FindProviders searches until the context expires.
func (dht *IpfsDHT) FindProviders(ctx context.Context, c cid.Cid) ([]peer.AddrInfo, error) {
	if !dht.enableProviders {
		return nil, routing.ErrNotSupported
	} else if !c.Defined() {
		return nil, fmt.Errorf("invalid cid: undefined")
	}

	var providers []peer.AddrInfo
	for p := range dht.FindProvidersAsync(ctx, c, dht.bucketSize) {
		providers = append(providers, p)
	}
	return providers, nil
}

// FindProvidersAsyncExtended searches until the context expires.
func (dht *IpfsDHT) FindProvidersAsyncExtended(ctx context.Context, key cid.Cid, opts ...routing.Option) (<-chan peer.AddrInfo, <-chan []peer.ID, error) {
	if !dht.enableProviders || !key.Defined() {
		peerOut, closestPeers := make(chan peer.AddrInfo), make(chan []peer.ID)
		close(peerOut)
		close(closestPeers)
		return peerOut, closestPeers, routing.ErrNotSupported
	}

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, nil, err
	}

	count := dhtrouting.GetQuorum(&cfg)
	seedPeerOpts := dhtrouting.GetSeedPeers(&cfg)

	chSize := count
	if count == 0 {
		chSize = 1
	}
	peerOut := make(chan peer.AddrInfo, chSize)
	closestPeersOut := make(chan []peer.ID, 1)

	keyMH := key.Hash()

	logger.Debugw("finding providers", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))
	go dht.findProvidersAsyncRoutine(ctx, keyMH, count, seedPeerOpts, peerOut, closestPeersOut)
	return peerOut, closestPeersOut, nil
}

// FindProvidersAsync is the same thing as FindProviders, but returns a channel.
// Peers will be returned on the channel as soon as they are found, even before
// the search query completes. If count is zero then the query will run until it
// completes. Note: not reading from the returned channel may block the query
// from progressing.
func (dht *IpfsDHT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	providers, _, _ := dht.FindProvidersAsyncExtended(ctx, key, dhtrouting.Quorum(count))
	return providers
}

func (dht *IpfsDHT) findProvidersAsyncRoutine(ctx context.Context, key multihash.Multihash, count int, seedPeerOpts dhtrouting.SeedPeersOptions, peerOut chan peer.AddrInfo, closestPeersOut chan []peer.ID) {
	defer close(peerOut)
	defer close(closestPeersOut)

	findAll := count == 0
	var ps *peer.Set
	if findAll {
		ps = peer.NewSet()
	} else {
		ps = peer.NewLimitedSet(count)
	}

	provs := dht.ProviderManager.GetProviders(ctx, key)
	for _, p := range provs {
		// NOTE: Assuming that this list of peers is unique
		if ps.TryAdd(p) {
			pi := dht.peerstore.PeerInfo(p)
			select {
			case peerOut <- pi:
			case <-ctx.Done():
				return
			}
		}

		// If we have enough peers locally, don't bother with remote RPC
		// TODO: is this a DOS vector?
		if !findAll && ps.Size() >= count {
			return
		}
	}

	lookupRes, err := dht.runLookupWithFollowup(ctx, string(key),
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			provs, closest, err := dht.protoMessenger.GetProviders(ctx, p, key)
			if err != nil {
				return nil, err
			}

			logger.Debugf("%d provider entries", len(provs))

			// Add unique providers from request, up to 'count'
			for _, prov := range provs {
				dht.maybeAddAddrs(prov.ID, prov.Addrs, peerstore.TempAddrTTL)
				logger.Debugf("got provider: %s", prov)
				if ps.TryAdd(prov.ID) {
					logger.Debugf("using provider: %s", prov)
					select {
					case peerOut <- *prov:
					case <-ctx.Done():
						logger.Debug("context timed out sending more providers")
						return nil, ctx.Err()
					}
				}
				if !findAll && ps.Size() >= count {
					logger.Debugf("got enough providers (%d/%d)", ps.Size(), count)
					return nil, nil
				}
			}

			// Give closer peers back to the query to be queried
			logger.Debugf("got closer peers: %d %s", len(closest), closest)

			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: closest,
			})

			return closest, nil
		},
		func() bool {
			return !findAll && ps.Size() >= count
		},
		seedPeerOpts.SeedPeers, seedPeerOpts.UseRTPeers,
	)

	if lookupRes != nil {
		closestPeersOut <- lookupRes.peers
	}

	if err == nil && ctx.Err() == nil {
		dht.refreshRTIfNoShortcut(kb.ConvertKey(string(key)), lookupRes)
	}
}

// FindPeerExtended searches for a peer with given ID.
func (dht *IpfsDHT) FindPeerExtended(ctx context.Context, id peer.ID, opts ...routing.Option) (peer.AddrInfo, []peer.ID, error) {
	if err := id.Validate(); err != nil {
		return peer.AddrInfo{}, nil, err
	}

	logger.Debugw("finding peer", "peer", id)

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return peer.AddrInfo{}, nil, err
	}
	seedPeerOpts := dhtrouting.GetSeedPeers(&cfg)

	// Check if were already connected to them
	if pi := dht.FindLocal(id); pi.ID != "" {
		return pi, nil, nil
	}

	lookupRes, err := dht.runLookupWithFollowup(ctx, string(id),
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, id)
			if err != nil {
				logger.Debugf("error getting closer peers: %s", err)
				return nil, err
			}

			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			return peers, err
		},
		func() bool {
			return dht.host.Network().Connectedness(id) == network.Connected
		},
		seedPeerOpts.SeedPeers, seedPeerOpts.UseRTPeers,
	)

	if err != nil {
		return peer.AddrInfo{}, nil, err
	}

	dialedPeerDuringQuery := false
	for i, p := range lookupRes.peers {
		if p == id {
			// Note: we consider PeerUnreachable to be a valid state because the peer may not support the DHT protocol
			// and therefore the peer would fail the query. The fact that a peer that is returned can be a non-DHT
			// server peer and is not identified as such is a bug.
			dialedPeerDuringQuery = (lookupRes.state[i] == qpeerset.PeerQueried || lookupRes.state[i] == qpeerset.PeerUnreachable || lookupRes.state[i] == qpeerset.PeerWaiting)
			break
		}
	}

	// Return peer information if we tried to dial the peer during the query or we are (or recently were) connected
	// to the peer.
	connectedness := dht.host.Network().Connectedness(id)
	if dialedPeerDuringQuery || connectedness == network.Connected || connectedness == network.CanConnect {
		return dht.peerstore.PeerInfo(id), lookupRes.peers, nil
	}

	return peer.AddrInfo{}, lookupRes.peers, routing.ErrNotFound
}

// FindPeer searches for a peer with given ID.
func (dht *IpfsDHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	pid, _, err := dht.FindPeerExtended(ctx, id)
	return pid, err
}
