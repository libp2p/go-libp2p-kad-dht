package dht

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	otel "go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

var _ routing.Routing = (*DHT)(nil)

func (d *DHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.FindPeer")
	defer span.End()

	// First check locally. If we are or were recently connected to the peer,
	// return the addresses from our peerstore unless the information doesn't
	// contain any.
	switch d.host.Network().Connectedness(id) {
	case network.Connected, network.CanConnect:
		addrInfo := d.host.Peerstore().PeerInfo(id)
		if addrInfo.ID != "" && len(addrInfo.Addrs) > 0 {
			return addrInfo, nil
		}
	default:
		// we're
	}

	target := kadt.PeerID(id)

	var foundPeer peer.ID
	fn := func(ctx context.Context, visited kadt.PeerID, msg *pb.Message, stats coordt.QueryStats) error {
		if peer.ID(visited) == id {
			foundPeer = peer.ID(visited)
			return coordt.ErrSkipRemaining
		}
		return nil
	}

	_, _, err := d.kad.QueryClosest(ctx, target.Key(), fn, 20)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("failed to run query: %w", err)
	}

	if foundPeer == "" {
		return peer.AddrInfo{}, fmt.Errorf("peer record not found")
	}

	return d.host.Peerstore().PeerInfo(foundPeer), nil
}

func (d *DHT) Provide(ctx context.Context, c cid.Cid, brdcst bool) error {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.Provide", otel.WithAttributes(attribute.String("cid", c.String())))
	defer span.End()

	// verify if this DHT supports provider records by checking if a "providers"
	// backend is registered.
	b, found := d.backends[namespaceProviders]
	if !found {
		return routing.ErrNotSupported
	}

	// verify that it's "defined" CID (not empty)
	if !c.Defined() {
		return fmt.Errorf("invalid cid: undefined")
	}

	// store ourselves as one provider for that CID
	_, err := b.Store(ctx, string(c.Hash()), peer.AddrInfo{ID: d.host.ID()})
	if err != nil {
		return fmt.Errorf("storing own provider record: %w", err)
	}

	// if broadcast is "false" we won't query the DHT
	if !brdcst {
		return nil
	}

	// construct message
	addrInfo := peer.AddrInfo{
		ID:    d.host.ID(),
		Addrs: d.host.Addrs(),
	}

	msg := &pb.Message{
		Type: pb.Message_ADD_PROVIDER,
		Key:  c.Hash(),
		ProviderPeers: []*pb.Message_Peer{
			pb.FromAddrInfo(addrInfo),
		},
	}

	// finally, find the closest peers to the target key.
	return d.kad.BroadcastRecord(ctx, msg)
}

func (d *DHT) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	peerOut := make(chan peer.AddrInfo)
	go d.findProvidersAsyncRoutine(ctx, c, count, peerOut)
	return peerOut
}

func (d *DHT) findProvidersAsyncRoutine(ctx context.Context, c cid.Cid, count int, out chan<- peer.AddrInfo) {
	_, span := d.tele.Tracer.Start(ctx, "DHT.findProvidersAsyncRoutine", otel.WithAttributes(attribute.String("cid", c.String()), attribute.Int("count", count)))
	defer span.End()

	defer close(out)

	// verify if this DHT supports provider records by checking
	// if a "providers" backend is registered.
	b, found := d.backends[namespaceProviders]
	if !found || !c.Defined() {
		span.RecordError(fmt.Errorf("no providers backend registered or CID undefined"))
		return
	}

	// first fetch the record locally
	stored, err := b.Fetch(ctx, string(c.Hash()))
	if err != nil {
		span.RecordError(err)
		d.log.Warn("Fetching value from provider store", slog.String("cid", c.String()), slog.String("err", err.Error()))
		return
	}

	ps, ok := stored.(*providerSet)
	if !ok {
		span.RecordError(err)
		d.log.Warn("Stored value is not a provider set", slog.String("cid", c.String()), slog.String("type", fmt.Sprintf("%T", stored)))
		return
	}

	// send all providers onto the out channel until the desired count
	// was reached. If no count was specified, continue with network lookup.
	providers := map[peer.ID]struct{}{}
	for _, provider := range ps.providers {
		providers[provider.ID] = struct{}{}

		select {
		case <-ctx.Done():
			return
		case out <- provider:
		}

		if count != 0 && len(providers) == count {
			return
		}
	}

	// Craft message to send to other peers
	msg := &pb.Message{
		Type: pb.Message_GET_PROVIDERS,
		Key:  c.Hash(),
	}

	// handle node response
	fn := func(ctx context.Context, id kadt.PeerID, resp *pb.Message, stats coordt.QueryStats) error {
		// loop through all providers that the remote peer returned
		for _, provider := range resp.ProviderAddrInfos() {

			// if we had already sent that peer on the channel -> do nothing
			if _, found := providers[provider.ID]; found {
				continue
			}

			// keep track that we will have sent this peer on the channel
			providers[provider.ID] = struct{}{}

			// actually send the provider information to the user
			select {
			case <-ctx.Done():
				return coordt.ErrSkipRemaining
			case out <- provider:
			}

			// if count is 0, we will wait until the query has exhausted the keyspace
			// if count isn't 0, we will stop if the number of providers we have sent
			// equals the number that the user has requested.
			if count != 0 && len(providers) == count {
				return coordt.ErrSkipRemaining
			}
		}

		return nil
	}

	_, err = d.kad.QueryMessage(ctx, msg, fn, 20) // TODO: parameterize
	if err != nil {
		span.RecordError(err)
		d.log.Warn("Failed querying", slog.String("cid", c.String()), slog.String("err", err.Error()))
		return
	}
}

// PutValue satisfies the [routing.Routing] interface and will add the given
// value to the k-closest nodes to keyStr. The parameter keyStr should have the
// format `/$namespace/$binary_id`. Namespace examples are `pk` or `ipns`. To
// identify the closest peers to keyStr, that complete string will be SHA256
// hashed.
func (d *DHT) PutValue(ctx context.Context, keyStr string, value []byte, opts ...routing.Option) error {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.PutValue")
	defer span.End()

	// first parse the routing options
	rOpt := routing.Options{} // routing config
	if err := rOpt.Apply(opts...); err != nil {
		return fmt.Errorf("apply routing options: %w", err)
	}

	// then always store the given value locally
	if err := d.putValueLocal(ctx, keyStr, value); err != nil {
		return fmt.Errorf("put value locally: %w", err)
	}

	// if the routing system should operate in offline mode, stop here
	if rOpt.Offline {
		return nil
	}

	// construct Kademlia-key. Yes, we hash the complete key string which
	// includes the namespace prefix.
	msg := &pb.Message{
		Type:   pb.Message_PUT_VALUE,
		Key:    []byte(keyStr),
		Record: record.MakePutRecord(keyStr, value),
	}

	// finally, find the closest peers to the target key.
	err := d.kad.BroadcastRecord(ctx, msg)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}

	return nil
}

// putValueLocal stores a value in the local datastore without reaching out to
// the network.
func (d *DHT) putValueLocal(ctx context.Context, key string, value []byte) error {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.PutValueLocal")
	defer span.End()

	ns, path, err := record.SplitKey(key)
	if err != nil {
		return fmt.Errorf("splitting key: %w", err)
	}

	b, found := d.backends[ns]
	if !found {
		return routing.ErrNotSupported
	}

	rec := record.MakePutRecord(key, value)
	rec.TimeReceived = time.Now().UTC().Format(time.RFC3339Nano)

	_, err = b.Store(ctx, path, rec)
	if err != nil {
		return fmt.Errorf("store record locally: %w", err)
	}

	return nil
}

func (d *DHT) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.GetValue")
	defer span.End()

	valueChan, err := d.SearchValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	var best []byte
	for val := range valueChan {
		best = val
	}

	if ctx.Err() != nil {
		return best, ctx.Err()
	}

	if best == nil {
		return nil, routing.ErrNotFound
	}

	return best, nil
}

type quorumOptionKey struct{}

func RoutingQuorum(n int) routing.Option {
	return func(opts *routing.Options) error {
		if n < 0 {
			return fmt.Errorf("quorum must not be negative")
		}

		if opts.Other == nil {
			opts.Other = make(map[interface{}]interface{}, 1)
		}

		opts.Other[quorumOptionKey{}] = n

		return nil
	}
}

// GetQuorum defaults to 0 if no option is found
func GetQuorum(opts *routing.Options) int {
	quorum, ok := opts.Other[quorumOptionKey{}].(int)
	if !ok {
		quorum = 0 // TODO: make configurable
	}
	return quorum
}

// SearchValue will search in the DHT for keyStr. keyStr must have the form
// `/$namespace/$binary_id`
func (d *DHT) SearchValue(ctx context.Context, keyStr string, options ...routing.Option) (<-chan []byte, error) {
	_, span := d.tele.Tracer.Start(ctx, "DHT.SearchValue")
	defer span.End()

	// first parse the routing options
	rOpt := &routing.Options{} // routing config
	if err := rOpt.Apply(options...); err != nil {
		return nil, fmt.Errorf("apply routing options: %w", err)
	}

	ns, path, err := record.SplitKey(keyStr)
	if err != nil {
		return nil, fmt.Errorf("splitting key: %w", err)
	}

	b, found := d.backends[ns]
	if !found {
		return nil, routing.ErrNotSupported
	}

	if rOpt.Offline {
		val, err := b.Fetch(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("fetch from backend: %w", err)
		}

		rec, ok := val.(*recpb.Record)
		if !ok {
			return nil, fmt.Errorf("expected *recpb.Record from backend, got: %T", val)
		}

		if rec == nil {
			return nil, fmt.Errorf("not found locally")
		}

		out := make(chan []byte, 1)
		defer close(out)
		out <- rec.GetValue()
		return out, nil
	}

	out := make(chan []byte)
	go d.searchValueRoutine(ctx, keyStr, rOpt, out)
	return out, nil
}

func (d *DHT) searchValueRoutine(ctx context.Context, keyStr string, ropt *routing.Options, out chan<- []byte) {
	_, span := d.tele.Tracer.Start(ctx, "DHT.searchValueRoutine")
	defer span.End()
	defer close(out)

	// TODO: remove below duplication
	ns, path, err := record.SplitKey(keyStr)
	if err != nil {
		return
	}

	b, found := d.backends[ns]
	if !found {
		return
	}

	req := &pb.Message{
		Type: pb.Message_GET_VALUE,
		Key:  []byte(keyStr),
	}

	//  The currently known best value for keyStr
	var best []byte

	// The peers that returned the best value
	quorumPeers := map[kadt.PeerID]struct{}{}

	// The quorum that we require for terminating the query. This number tells
	// us how many peers must have responded with the "best" value before we
	// cancel the query.
	quorum := GetQuorum(ropt)

	fn := func(ctx context.Context, id kadt.PeerID, resp *pb.Message, stats coordt.QueryStats) error {
		rec := resp.GetRecord()
		if rec == nil {
			return nil
		}

		if !bytes.Equal([]byte(keyStr), rec.GetKey()) {
			d.log.Debug("record key mismatch")
			return nil
		}

		idx, _ := b.Validate(ctx, path, best, rec.GetValue())
		switch idx {
		case 0:
			if bytes.Equal(best, rec.GetValue()) {
				quorumPeers[id] = struct{}{}
			}
		case 1:
			best = rec.GetValue()
			quorumPeers = map[kadt.PeerID]struct{}{}
			quorumPeers[id] = struct{}{}
			out <- rec.GetValue()
		case -1:
			// no valid value found yet
			return nil
		default:
			d.log.Warn("unexpected validate index", slog.Int("idx", idx))
		}

		// Check if we have reached the quorum
		if len(quorumPeers) == quorum {
			return coordt.ErrSkipRemaining
		}

		return nil
	}

	_, err = d.kad.QueryMessage(ctx, req, fn, d.cfg.BucketSize)
	if err != nil {
		d.log.Warn("Search value query failed", slog.String("err", err.Error()))
	}
}

func (d *DHT) Bootstrap(ctx context.Context) error {
	ctx, span := d.tele.Tracer.Start(ctx, "DHT.Bootstrap")
	defer span.End()

	seed := make([]kadt.PeerID, len(d.cfg.BootstrapPeers))
	for i, addrInfo := range d.cfg.BootstrapPeers {
		seed[i] = kadt.PeerID(addrInfo.ID)
		// TODO: how to handle TTL if BootstrapPeers become dynamic and don't
		// point to stable peers or consist of ephemeral peers that we have
		// observed during a previous run.
		d.host.Peerstore().AddAddrs(addrInfo.ID, addrInfo.Addrs, peerstore.PermanentAddrTTL)
	}

	return d.kad.Bootstrap(ctx, seed)
}
