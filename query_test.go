package dht

import (
	"context"
	"sort"
	"sync"
	"testing"

	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	ks "github.com/whyrusleeping/go-keyspace"
)

type nextStep func(peer.ID, int) (*dhtQueryResult, bool)

func createDisjiontTracks(t *testing.T, hosts []host.Host, goodLength int) (peer.ID, []peer.ID, nextStep) {
	myKey := ks.XORKeySpace.Key([]byte(hosts[0].ID()))

	sorted := make([]host.Host, len(hosts))
	copy(sorted, hosts)
	// sort close to far (relative to us)
	sort.Slice(sorted, func(i, j int) bool {
		left := myKey.Distance(ks.XORKeySpace.Key([]byte(sorted[i].ID())))
		right := myKey.Distance(ks.XORKeySpace.Key([]byte(sorted[j].ID())))
		return left.Cmp(right) == -1
	})

	target := sorted[len(sorted)-1]
	sorted = sorted[1:] // remove us

	goodTrack := make([]host.Host, goodLength)
	copy(goodTrack, sorted[0:goodLength])
	goodTrack = append(goodTrack, target)
	badTrack := sorted[goodLength : len(sorted)-1]

	if len(badTrack) <= len(goodTrack) {
		t.Fatal("insufficient number of peers for disjoint track test")
	}

	tracks := [][]host.Host{goodTrack, badTrack}
	next := func(id peer.ID, trackNum int) (*dhtQueryResult, bool) {
		track := tracks[trackNum]
		pos := -1
		for i, v := range track {
			if v.ID() == id {
				pos = i
			}
		}
		if pos == -1 {
			return nil, false
		}

		nextPos := pos + 1
		// if we're at the end of the track
		if nextPos == len(track) {
			if trackNum == 0 { // good track; success
				return &dhtQueryResult{
					success: true,
				}, true
			} else { // bad track; dead end
				return &dhtQueryResult{
					closerPeers: []*pstore.PeerInfo{},
				}, true
			}
		} else {
			closerPeers := make([]*pstore.PeerInfo, 1)
			closerPeers[0] = host.PeerInfoFromHost(track[nextPos])

			return &dhtQueryResult{
				closerPeers: closerPeers,
			}, false
		}
	}

	return target.ID(), []peer.ID{goodTrack[0].ID(), badTrack[0].ID()}, next
}

/*
 * This test creates two disjoint tracks of peers, one for
 * each of the query's two paths to follow. The "good"
 * track that leads to the target initially has high
 * distances to the target, while the "bad" track that
 * goes nowhere has small distances to the target.
 * Only by going down both simultaneously will it find
 * the target before the end of the bad track. The greedy
 * behavior without disjoint paths would reach the target
 * only after visiting every single peer.
 *
 *                 xor distance to target
 * far <-----------------------------------------------> close
 * <us>
 *     <good 0> <g 1> <g 2>                            <target>
 *                           <bad 0> <b 1> ... <b n>
 *
 */
func TestQueryDisjoint(t *testing.T) {
	goodLength := 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// This test unavoidably contains a race between the two paths.
	// Making totalPeers higher lowers the chance of false failure,
	// at a slight cost to test runtime.
	totalPeers := 25
	mn, err := mocknet.FullMeshConnected(ctx, totalPeers)
	if err != nil {
		t.Fatal(err)
	}

	hosts := mn.Hosts()
	target, starts, next := createDisjiontTracks(t, hosts, goodLength)

	dht, err := New(ctx, hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	var mux sync.Mutex
	badEndVisited := false

	query := dht.newQuery(string(target), func(pathIndex int, numPaths int) queryFunc {
		return func(ctx context.Context, id peer.ID) (*dhtQueryResult, error) {
			res, end := next(id, pathIndex)
			if res == nil {
				t.Fatal("Unexpected peer visited")
			}
			if end {
				mux.Lock()
				if !res.success {
					badEndVisited = true
				} else {
					if badEndVisited {
						t.Fatal("Visited wrong path end first")
					}
				}
				mux.Unlock()
			}

			return res, nil
		}
	})
	query.concurrency = 1

	res, err := query.Run(ctx, starts)
	if err != nil {
		t.Fatal(err)
	}

	if res.finalSet.Size() != totalPeers-1 {
		t.Fatal("Incorrect number of peers queried")
	}

	if len(res.paths) != 1 {
		t.Fatal("Expected one successful path")
	}
}
