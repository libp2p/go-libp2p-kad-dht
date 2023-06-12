package dht

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/netsize"
	"github.com/libp2p/go-libp2p/core/peer"
)

func randInt(rng *rand.Rand, n, except int) int {
	for {
		r := rng.Intn(n)
		if r != except {
			return r
		}
	}
}

func TestOptimisticProvide(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Order of events:
	// 1. setup DHTs
	// 2. connect each DHT with three others (but not to itself)
	// 3. select random DHT to be the privileged one (performs the optimistic provide)
	// 4. initialize network size estimator of privileged DHT
	// 5. perform provides
	// 6. let all other DHTs perform the lookup for all provided CIDs

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhtCount := 21

	dhts := setupDHTS(t, ctx, dhtCount, EnableOptimisticProvide())
	defer func() {
		for i := 0; i < dhtCount; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	// connect each DHT with three random others
	for i, dht := range dhts {
		for j := 0; j < 3; j++ {
			r := randInt(rng, dhtCount, i)
			connect(t, ctx, dhts[r], dht)
		}
	}

	// select privileged DHT that will perform the provide operation
	privIdx := rng.Intn(dhtCount)
	privDHT := dhts[privIdx]

	peerIDs := make([]peer.ID, 20)
	for i := 0; i < dhtCount; i++ {
		if i == privIdx {
			continue
		}

		if i >= privIdx {
			peerIDs[i-1] = dhts[i-1].self
		} else {
			peerIDs[i] = dhts[i].self
		}
	}
	nse := netsize.NewEstimator(privDHT.self, privDHT.routingTable, privDHT.bucketSize)

	for i := 0; i < 20; i++ {
		err := nse.Track(string(testCaseCids[i].Bytes()), peerIDs)
		if err != nil {
			t.Fatal(err)
		}
	}
	privDHT.nsEstimator = nse

	for _, k := range testCaseCids {
		logger.Debugf("announcing provider for %s", k)
		if err := privDHT.optimisticProvide(ctx, k.Hash()); err != nil {
			t.Fatal(err)
		}
	}

	for _, c := range testCaseCids {
		n := randInt(rng, dhtCount, privIdx)

		ctxT, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		provchan := dhts[n].FindProvidersAsync(ctxT, c, 1)

		select {
		case prov := <-provchan:
			if prov.ID == "" {
				t.Fatal("Got back nil provider")
			}
			if prov.ID != privDHT.self {
				t.Fatal("Got back wrong provider")
			}
		case <-ctxT.Done():
			t.Fatal("Did not get a provider back.")
		}
	}
}
