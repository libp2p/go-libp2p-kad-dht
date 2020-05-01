package dht

import (
	"context"
	"sync"

	"github.com/ipfs/go-cid"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kbucket "github.com/libp2p/go-libp2p-kbucket"

	"github.com/libp2p/go-libp2p-core/helpers"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	xork "github.com/libp2p/go-libp2p-xor/kademlia"
	"github.com/libp2p/go-libp2p-xor/key"
	"github.com/libp2p/go-libp2p-xor/trie"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

const maxEnumerationWorkers = 100

func (dht *IpfsDHT) enumerate(ctx context.Context) (map[peer.ID]ma.Multiaddr, error) {
	seen := make(map[peer.ID]struct{})
	succeeded := make(map[peer.ID]ma.Multiaddr)

	toDial := pstore.PeerInfos(dht.peerstore, dht.routingTable.ListPeers())

	type result struct {
		id      peer.ID
		addr    ma.Multiaddr
		closer  []*peer.AddrInfo
		success bool
	}

	jobs := make(chan peer.AddrInfo)
	results := make(chan result)

	var wg sync.WaitGroup
	wg.Add(maxEnumerationWorkers)
	for i := 0; i < maxEnumerationWorkers; i++ {
		go func() {
			defer wg.Done()
			for ai := range jobs {
				dht.peerstore.AddAddrs(ai.ID, ai.Addrs, pstore.TempAddrTTL)
				res := result{id: ai.ID}
				if ms, err := dht.messageSenderForPeer(ctx, ai.ID); err != nil {
					// failure
				} else if mes, err := dht.findPeerSingle(ctx, ai.ID, ai.ID); err != nil {
					// failure
				} else {
					res.addr = ms.s.Conn().RemoteMultiaddr()
					res.closer = pb.PBPeersToPeerInfos(mes.GetCloserPeers())
					res.success = true
				}
				results <- res
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	defer func() {
		close(jobs)
		for range results {
		}
	}()

	processResult := func(res result) {
		if res.success {
			succeeded[res.id] = res.addr
			for _, ai := range res.closer {
				if _, ok := seen[ai.ID]; !ok {
					seen[ai.ID] = struct{}{}
					toDial = append(toDial, *ai)
				}
			}
		}
	}

	outstanding := 0
	for len(toDial) > 0 && outstanding > 0 {
		if len(toDial) > 0 {
			select {
			case res := <-results:
				outstanding--
				processResult(res)
			case jobs <- toDial[len(toDial)-1]:
				toDial = toDial[:len(toDial)-1]
				outstanding++
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else {
			select {
			case res := <-results:
				outstanding--
				processResult(res)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return succeeded, nil
}

type networkTrie struct {
	tree    *trie.Trie
	addrs   map[peer.ID]ma.Multiaddr
	mapping map[string]peer.ID
}

func (n *networkTrie) split(hashes []multihash.Multihash, k int) map[peer.ID][]multihash.Multihash {
	split := make(map[peer.ID][]multihash.Multihash, len(n.addrs))
	for _, hash := range hashes {
		key := key.KbucketIDToKey(kbucket.ConvertKey(string(hash)))
		for _, pk := range xork.ClosestN(key, n.tree, k) {
			p := n.mapping[string(pk)]
			split[p] = append(split[p], hash)
		}
	}
	return split
}

func buildTrie(addrs map[peer.ID]ma.Multiaddr) *networkTrie {
	tree := &networkTrie{
		tree:    trie.New(),
		addrs:   addrs,
		mapping: make(map[string]peer.ID, len(addrs)),
	}
	for pid := range tree.addrs {
		k := key.KbucketIDToKey(kbucket.ConvertPeerID(pid))
		tree.tree.Add(k)
		tree.mapping[string(k)] = pid
	}
	return tree
}

func (dht *IpfsDHT) ProvideAll(ctx context.Context, keys <-chan cid.Cid, brdcst bool) error {
	if !dht.enableProviders {
		return routing.ErrNotSupported
	}

	if !brdcst {
		return dht.provideAllLocal(ctx, keys)
	}

	allPeers, err := dht.enumerate(ctx)
	if err != nil {
		return err // context canceled.
	}

	tree := buildTrie(allPeers)

	batch := make([]multihash.Multihash, 0, len(allPeers))
	batchMax := len(allPeers) * 100

	done := false
	for !done && ctx.Err() == nil {
	batch:
		for len(batch) < batchMax {
			select {
			case key, ok := <-keys:
				if !ok {
					done = true
					break batch
				}
				keyMH := key.Hash()
				dht.ProviderManager.AddProvider(ctx, keyMH, dht.self)
				batch = append(batch, keyMH)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		dht.provideParallel(ctx, tree, batch)
	}
	return nil
}

// TODO: make this parallel
func (dht *IpfsDHT) provideParallel(
	ctx context.Context,
	tree *networkTrie,
	batch []multihash.Multihash,
) error {

	providers := pb.RawPeerInfosToPBPeers([]peer.AddrInfo{
		peer.AddrInfo{ID: dht.self, Addrs: dht.host.Addrs()},
	})

	for p, hashes := range tree.split(batch, dht.bucketSize) {
		dht.peerstore.AddAddr(p, tree.addrs[p], pstore.TempAddrTTL)
		dht.provideMany(ctx, providers, p, hashes)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (dht *IpfsDHT) provideMany(
	ctx context.Context,
	providers []pb.Message_Peer,
	p peer.ID,
	hashes []multihash.Multihash,
) {
	s, err := dht.host.NewStream(ctx, p, dht.protocols...)
	if err != nil {
		// trying is good enough.
		return
	}
	defer s.Reset()

	var mes pb.Message
	bw := writerPool.Get().(*bufferedDelimitedWriter)
	bw.Reset(s)
	defer func() {
		bw.Reset(nil)
		writerPool.Put(bw)
	}()

	mes.Type = pb.Message_ADD_PROVIDER
	mes.ProviderPeers = providers
	for _, h := range hashes {
		mes.Key = h
		err := bw.WriteMsg(&mes)
		if err != nil {
			return
		}
	}
	err = bw.Flush()
	if err != nil {
		return
	}
	_ = helpers.FullClose(s)
}
