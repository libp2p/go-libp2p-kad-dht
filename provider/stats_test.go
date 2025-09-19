//go:build go1.25
// +build go1.25

package provider

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	kb "github.com/libp2p/go-libp2p-kbucket"

	"github.com/stretchr/testify/require"
)

func TestStats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		replicationFactor := 4
		peerPrefixBitlen := 7
		require.LessOrEqual(t, peerPrefixBitlen, bitsPerByte)

		reprovideInterval := time.Hour

		var nPeers byte = 1 << peerPrefixBitlen // 2**peerPrefixBitlen
		// Generate balanced peers over the prefix space
		peers := make([]peer.ID, nPeers)
		var err error
		for i := range nPeers {
			b := i << (bitsPerByte - peerPrefixBitlen)
			k := [32]byte{b}
			peers[i], err = kb.GenRandPeerIDWithCPL(k[:], uint(peerPrefixBitlen))
			require.NoError(t, err)
		}
		reachabilityModulo := 8 // 1/8 peers unreachable
		peerReachability := make(map[peer.ID]bool)
		for i, p := range peers {
			peerReachability[p] = i%reachabilityModulo != 0
		}

		pid, err := peer.Decode("12D3KooWCPQTeFYCDkru8nza3Af6u77aoVLA71Vb74eHxeR91Gka") // kadid starts with 16*"0"
		require.NoError(t, err)

		router := &mockRouter{
			getClosestPeersFunc: func(ctx context.Context, k string) ([]peer.ID, error) {
				sortedPeers := kb.SortClosestPeers(peers, kb.ConvertKey(k))
				return sortedPeers[:min(replicationFactor, len(peers))], nil
			},
		}
		blocked := false
		// blockedLk := sync.Mutex{}
		blockedCond := sync.NewCond(&sync.Mutex{})
		msgSender := &mockMsgSender{
			sendMessageFunc: func(ctx context.Context, p peer.ID, m *pb.Message) error {
				blockedCond.L.Lock()
				for blocked {
					blockedCond.Wait()
				}
				blockedCond.L.Unlock()
				if reachable := peerReachability[p]; !reachable {
					return errors.New("peer not reachable")
				}
				return nil
			},
		}

		maxWorkers := 12
		dedicatedBurstWorkers := 6
		dedicatedPeriodicWorkers := 2
		maxConnsPerWorker := 3
		offlineDelay := 10 * time.Minute

		startTime := time.Now()

		opts := []Option{
			WithReprovideInterval(reprovideInterval),
			WithReplicationFactor(replicationFactor),
			WithMaxWorkers(maxWorkers),
			WithDedicatedBurstWorkers(dedicatedBurstWorkers),
			WithDedicatedPeriodicWorkers(dedicatedPeriodicWorkers),
			WithMaxProvideConnsPerWorker(maxConnsPerWorker),
			WithPeerID(pid),
			WithRouter(router),
			WithMessageSender(msgSender),
			WithSelfAddrs(func() []ma.Multiaddr {
				addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
				require.NoError(t, err)
				return []ma.Multiaddr{addr}
			}),
			WithOfflineDelay(offlineDelay),
		}
		prov, err := New(opts...)
		require.NoError(t, err)
		synctest.Wait()

		require.True(t, prov.connectivity.IsOnline())

		// Wait 1 minute and do nothing
		time.Sleep(time.Minute)

		avgPrefixLen := 5

		// Initial stats check
		stats := prov.Stats()
		require.False(t, stats.Closed)
		require.Equal(t, "online", stats.Connectivity.Status)
		require.Equal(t, startTime, stats.Connectivity.Since)
		require.Equal(t, 0, stats.Queues.PendingKeyProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionReprovides)
		require.Equal(t, 0, stats.Schedule.Keys)
		require.Equal(t, 0, stats.Schedule.Regions)
		require.Equal(t, avgPrefixLen, stats.Schedule.AvgPrefixLength)
		require.Equal(t, bitstr.Key(""), stats.Schedule.NextReprovidePrefix)
		require.Equal(t, time.Time{}, stats.Schedule.NextReprovideAt)
		require.Equal(t, maxWorkers, stats.Workers.Max)
		require.Equal(t, 0, stats.Workers.Active)
		require.Equal(t, 0, stats.Workers.ActiveBurst)
		require.Equal(t, 0, stats.Workers.ActivePeriodic)
		require.Equal(t, dedicatedBurstWorkers, stats.Workers.DedicatedBurst)
		require.Equal(t, dedicatedPeriodicWorkers, stats.Workers.DedicatedPeriodic)
		require.Equal(t, 0, stats.Workers.QueuedBurst)
		require.Equal(t, 0, stats.Workers.QueuedPeriodic)
		require.Equal(t, maxConnsPerWorker, stats.Workers.MaxProvideConnsPerWorker)
		require.Equal(t, time.Since(startTime), stats.Timing.Uptime)
		require.Equal(t, reprovideInterval, stats.Timing.ReprovidesInterval)
		require.Equal(t, time.Since(startTime), stats.Timing.CurrentTimeOffset)
		require.Equal(t, DefaultMaxReprovideDelay, stats.Timing.MaxReprovideDelay)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyReprovides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionReprovides)
		require.Equal(t, 0, stats.Operations.Past.RecordsProvided)
		require.Equal(t, 0, stats.Operations.Past.KeysProvided)
		require.Equal(t, 0, stats.Operations.Past.KeysFailed)
		require.Equal(t, 0., stats.Operations.Past.KeysProvidedPerMinute)
		require.Equal(t, 0., stats.Operations.Past.KeysReprovidedPerMinute)
		require.Equal(t, time.Duration(0), stats.Operations.Past.RegionReprovideDuration)
		require.Equal(t, 0., stats.Operations.Past.AvgKeysPerReprovide)
		require.Equal(t, 0, stats.Operations.Past.RegionReprovidedLastCycle)
		require.Equal(t, 0, stats.Network.Peers)
		require.Equal(t, 0, stats.Network.Reachable)
		require.False(t, stats.Network.CompleteKeyspaceCoverage)
		require.Equal(t, 0., stats.Network.AvgHolders)
		require.Equal(t, replicationFactor, stats.Network.ReplicationFactor)

		wg := sync.WaitGroup{}

		// Block the provide operation
		blockedCond.L.Lock()
		blocked = true
		blockedCond.L.Unlock()

		h, err := multihash.FromB58String("QmTQjzSazBrNFUiNkDEBFyJXrEBiQj55dBEcthNgSJm6pV")
		require.NoError(t, err)
		require.True(t, keyspace.IsPrefix(bitstr.Key("0000000"), keyspace.MhToBit256(h)))

		wg.Go(func() {
			err := prov.StartProviding(false, h)
			require.NoError(t, err)
		})

		synctest.Wait()

		// Check stats during a provide operation (blocked)
		stats = prov.Stats()

		require.False(t, stats.Closed)
		require.Equal(t, "online", stats.Connectivity.Status)
		require.Equal(t, startTime, stats.Connectivity.Since)
		require.Equal(t, 0, stats.Queues.PendingKeyProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionReprovides)
		require.Equal(t, 1, stats.Schedule.Keys)
		require.Equal(t, 1, stats.Schedule.Regions)
		require.Equal(t, avgPrefixLen, stats.Schedule.AvgPrefixLength)
		require.Equal(t, bitstr.Key(strings.Repeat("0", avgPrefixLen)), stats.Schedule.NextReprovidePrefix)
		require.Equal(t, startTime.Add(reprovideInterval), stats.Schedule.NextReprovideAt)
		require.Equal(t, maxWorkers, stats.Workers.Max)
		require.Equal(t, 1, stats.Workers.Active)
		require.Equal(t, 1, stats.Workers.ActiveBurst)
		require.Equal(t, 0, stats.Workers.ActivePeriodic)
		require.Equal(t, dedicatedBurstWorkers, stats.Workers.DedicatedBurst)
		require.Equal(t, dedicatedPeriodicWorkers, stats.Workers.DedicatedPeriodic)
		require.Equal(t, 0, stats.Workers.QueuedBurst)
		require.Equal(t, 0, stats.Workers.QueuedPeriodic)
		require.Equal(t, maxConnsPerWorker, stats.Workers.MaxProvideConnsPerWorker)
		require.Equal(t, time.Since(startTime), stats.Timing.Uptime)
		require.Equal(t, reprovideInterval, stats.Timing.ReprovidesInterval)
		require.Equal(t, time.Since(startTime), stats.Timing.CurrentTimeOffset)
		require.Equal(t, DefaultMaxReprovideDelay, stats.Timing.MaxReprovideDelay)
		require.Equal(t, 1, stats.Operations.Ongoing.KeyProvides)
		require.Equal(t, 1, stats.Operations.Ongoing.RegionProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyReprovides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionReprovides)
		require.Equal(t, 0, stats.Operations.Past.RecordsProvided)
		require.Equal(t, 0, stats.Operations.Past.KeysProvided)
		require.Equal(t, 0, stats.Operations.Past.KeysFailed)
		require.Equal(t, 0., stats.Operations.Past.KeysProvidedPerMinute)
		require.Equal(t, 0., stats.Operations.Past.KeysReprovidedPerMinute)
		require.Equal(t, time.Duration(0), stats.Operations.Past.RegionReprovideDuration)
		require.Equal(t, 0., stats.Operations.Past.AvgKeysPerReprovide)
		require.Equal(t, 0, stats.Operations.Past.RegionReprovidedLastCycle)
		require.Equal(t, 0, stats.Network.Peers)
		require.Equal(t, 0, stats.Network.Reachable)
		require.False(t, stats.Network.CompleteKeyspaceCoverage)
		require.Equal(t, 0., stats.Network.AvgHolders)
		require.Equal(t, replicationFactor, stats.Network.ReplicationFactor)

		// Sleep 1 minute, so that we provide 1 key per minute
		time.Sleep(time.Minute)

		blockedCond.L.Lock()
		blocked = false
		blockedCond.Broadcast()
		blockedCond.L.Unlock()
		wg.Wait()
		synctest.Wait()

		// Check stats after the provide operation
		stats = prov.Stats()

		// Count to how many peers the record was actually provided
		recordsProvided := 0
		sollicitedPeers := make(map[peer.ID]struct{})
		reachablePeers := make(map[peer.ID]struct{})
		closestPeers, err := router.GetClosestPeers(context.Background(), string(h))
		require.NoError(t, err)
		for _, p := range closestPeers {
			sollicitedPeers[p] = struct{}{}
			if peerReachability[p] {
				recordsProvided++
				reachablePeers[p] = struct{}{}
			}
		}

		require.False(t, stats.Closed)
		require.Equal(t, "online", stats.Connectivity.Status)
		require.Equal(t, startTime, stats.Connectivity.Since)
		require.Equal(t, 0, stats.Queues.PendingKeyProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionReprovides)
		require.Equal(t, 1, stats.Schedule.Keys)
		require.Equal(t, 1, stats.Schedule.Regions)
		require.Equal(t, avgPrefixLen, stats.Schedule.AvgPrefixLength)
		require.Equal(t, bitstr.Key(strings.Repeat("0", avgPrefixLen)), stats.Schedule.NextReprovidePrefix)
		require.Equal(t, startTime.Add(reprovideInterval), stats.Schedule.NextReprovideAt)
		require.Equal(t, maxWorkers, stats.Workers.Max)
		require.Equal(t, 0, stats.Workers.Active)
		require.Equal(t, 0, stats.Workers.ActiveBurst)
		require.Equal(t, 0, stats.Workers.ActivePeriodic)
		require.Equal(t, dedicatedBurstWorkers, stats.Workers.DedicatedBurst)
		require.Equal(t, dedicatedPeriodicWorkers, stats.Workers.DedicatedPeriodic)
		require.Equal(t, 0, stats.Workers.QueuedBurst)
		require.Equal(t, 0, stats.Workers.QueuedPeriodic)
		require.Equal(t, maxConnsPerWorker, stats.Workers.MaxProvideConnsPerWorker)
		require.Equal(t, time.Since(startTime), stats.Timing.Uptime)
		require.Equal(t, reprovideInterval, stats.Timing.ReprovidesInterval)
		require.Equal(t, time.Since(startTime), stats.Timing.CurrentTimeOffset)
		require.Equal(t, DefaultMaxReprovideDelay, stats.Timing.MaxReprovideDelay)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyReprovides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionReprovides)
		require.Equal(t, recordsProvided, stats.Operations.Past.RecordsProvided)
		require.Equal(t, 1, stats.Operations.Past.KeysProvided)
		require.Equal(t, 0, stats.Operations.Past.KeysFailed)
		require.Equal(t, 1., stats.Operations.Past.KeysProvidedPerMinute)
		require.Equal(t, 0., stats.Operations.Past.KeysReprovidedPerMinute)
		require.Equal(t, time.Duration(0), stats.Operations.Past.RegionReprovideDuration)
		require.Equal(t, 0., stats.Operations.Past.AvgKeysPerReprovide)
		require.Equal(t, 0, stats.Operations.Past.RegionReprovidedLastCycle)
		require.Equal(t, 0, stats.Network.Peers)
		require.Equal(t, 0, stats.Network.Reachable)
		require.False(t, stats.Network.CompleteKeyspaceCoverage)
		require.Equal(t, float64(recordsProvided), stats.Network.AvgHolders)
		require.Equal(t, replicationFactor, stats.Network.ReplicationFactor)

		// Add more keys from the same keyspace region as `h` to the keystore. They
		// will all be reprovided together. Prefix is "0000000".
		newKeys := make([]multihash.Multihash, len(b58KeysPrefix0000000))
		for i, k := range b58KeysPrefix0000000 {
			newKeys[i], err = multihash.FromB58String(k)
			require.NoError(t, err)
			require.True(t, keyspace.IsPrefix(bitstr.Key("0000000"), keyspace.MhToBit256(newKeys[i])))
		}
		uniqueNewKeys, err := prov.keystore.Put(context.Background(), newKeys...)
		require.NoError(t, err)
		require.Len(t, uniqueNewKeys, len(newKeys)) // all keys are new

		// Block the reprovide operation
		blockedCond.L.Lock()
		blocked = true
		blockedCond.L.Unlock()

		// Sleep until the keys must be reprovided
		time.Sleep(time.Until(startTime.Add(reprovideInterval)))

		// Run reprovide until it blocks
		synctest.Wait()

		stats = prov.Stats()

		reprovidedKeys := len(newKeys) + 1

		require.False(t, stats.Closed)
		require.Equal(t, "online", stats.Connectivity.Status)
		require.Equal(t, startTime, stats.Connectivity.Since)
		require.Equal(t, 0, stats.Queues.PendingKeyProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionReprovides)
		require.Equal(t, reprovidedKeys, stats.Schedule.Keys)
		require.Equal(t, 0, stats.Schedule.Regions) // during the reprovide region is removed from schedule, it will be added back after reprovide is complete
		require.Equal(t, avgPrefixLen, stats.Schedule.AvgPrefixLength)
		require.Equal(t, bitstr.Key(strings.Repeat("0", avgPrefixLen)), stats.Schedule.NextReprovidePrefix)
		require.Equal(t, time.Time{}, stats.Schedule.NextReprovideAt) // region removed from schedule
		require.Equal(t, maxWorkers, stats.Workers.Max)
		require.Equal(t, 1, stats.Workers.Active)
		require.Equal(t, 0, stats.Workers.ActiveBurst)
		require.Equal(t, 1, stats.Workers.ActivePeriodic)
		require.Equal(t, dedicatedBurstWorkers, stats.Workers.DedicatedBurst)
		require.Equal(t, dedicatedPeriodicWorkers, stats.Workers.DedicatedPeriodic)
		require.Equal(t, 0, stats.Workers.QueuedBurst)
		require.Equal(t, 0, stats.Workers.QueuedPeriodic)
		require.Equal(t, maxConnsPerWorker, stats.Workers.MaxProvideConnsPerWorker)
		require.Equal(t, time.Since(startTime), stats.Timing.Uptime)
		require.Equal(t, reprovideInterval, stats.Timing.ReprovidesInterval)
		require.Equal(t, time.Since(startTime)%reprovideInterval, stats.Timing.CurrentTimeOffset)
		require.Equal(t, DefaultMaxReprovideDelay, stats.Timing.MaxReprovideDelay)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionProvides)
		require.Equal(t, reprovidedKeys, stats.Operations.Ongoing.KeyReprovides)
		require.Equal(t, 1, stats.Operations.Ongoing.RegionReprovides)
		require.Equal(t, recordsProvided, stats.Operations.Past.RecordsProvided)
		require.Equal(t, 1, stats.Operations.Past.KeysProvided)
		require.Equal(t, 0, stats.Operations.Past.KeysFailed)
		require.Equal(t, 1., stats.Operations.Past.KeysProvidedPerMinute)
		require.Equal(t, 0., stats.Operations.Past.KeysReprovidedPerMinute)
		require.Equal(t, time.Duration(0), stats.Operations.Past.RegionReprovideDuration)
		require.Equal(t, 0., stats.Operations.Past.AvgKeysPerReprovide)
		require.Equal(t, 0, stats.Operations.Past.RegionReprovidedLastCycle)
		require.Equal(t, 0, stats.Network.Peers)
		require.Equal(t, 0, stats.Network.Reachable)
		require.False(t, stats.Network.CompleteKeyspaceCoverage)
		require.Equal(t, float64(recordsProvided), stats.Network.AvgHolders)
		require.Equal(t, replicationFactor, stats.Network.ReplicationFactor)

		// Reprovide takes 2 minutes
		reprovideDuration := 2 * time.Minute
		time.Sleep(reprovideDuration)

		// Unblock the reprovide operation
		blockedCond.L.Lock()
		blocked = false
		blockedCond.Broadcast()
		blockedCond.L.Unlock()
		synctest.Wait()

		// Check stats after the reprovide operation
		stats = prov.Stats()

		newKeysProvidedRecords := 0
		for _, k := range newKeys {
			closestPeers, err := router.GetClosestPeers(context.Background(), string(k))
			require.NoError(t, err)
			for _, p := range closestPeers {
				sollicitedPeers[p] = struct{}{}
				if peerReachability[p] {
					newKeysProvidedRecords++
					reachablePeers[p] = struct{}{}
				}
			}
		}

		require.False(t, stats.Closed)
		require.Equal(t, "online", stats.Connectivity.Status)
		require.Equal(t, startTime, stats.Connectivity.Since)
		require.Equal(t, 0, stats.Queues.PendingKeyProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionReprovides)
		require.Equal(t, reprovidedKeys, stats.Schedule.Keys)
		require.Equal(t, 1, stats.Schedule.Regions)
		require.Equal(t, avgPrefixLen, stats.Schedule.AvgPrefixLength)
		require.Equal(t, bitstr.Key(strings.Repeat("0", avgPrefixLen)), stats.Schedule.NextReprovidePrefix)
		require.Equal(t, startTime.Add(2*reprovideInterval), stats.Schedule.NextReprovideAt)
		require.Equal(t, maxWorkers, stats.Workers.Max)
		require.Equal(t, 0, stats.Workers.Active)
		require.Equal(t, 0, stats.Workers.ActiveBurst)
		require.Equal(t, 0, stats.Workers.ActivePeriodic)
		require.Equal(t, dedicatedBurstWorkers, stats.Workers.DedicatedBurst)
		require.Equal(t, dedicatedPeriodicWorkers, stats.Workers.DedicatedPeriodic)
		require.Equal(t, 0, stats.Workers.QueuedBurst)
		require.Equal(t, 0, stats.Workers.QueuedPeriodic)
		require.Equal(t, maxConnsPerWorker, stats.Workers.MaxProvideConnsPerWorker)
		require.Equal(t, time.Since(startTime), stats.Timing.Uptime)
		require.Equal(t, reprovideInterval, stats.Timing.ReprovidesInterval)
		require.Equal(t, time.Since(startTime)%reprovideInterval, stats.Timing.CurrentTimeOffset)
		require.Equal(t, DefaultMaxReprovideDelay, stats.Timing.MaxReprovideDelay)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyReprovides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionReprovides)
		require.Equal(t, newKeysProvidedRecords+2*recordsProvided, stats.Operations.Past.RecordsProvided)
		require.Equal(t, reprovidedKeys+1, stats.Operations.Past.KeysProvided) // 1 provided, 6 reprovided
		require.Equal(t, 0, stats.Operations.Past.KeysFailed)
		require.Equal(t, 1., stats.Operations.Past.KeysProvidedPerMinute)
		require.Equal(t, float64(reprovidedKeys)/float64(reprovideDuration.Minutes()), stats.Operations.Past.KeysReprovidedPerMinute)
		require.Equal(t, reprovideDuration, stats.Operations.Past.RegionReprovideDuration)
		require.Equal(t, float64(reprovidedKeys), stats.Operations.Past.AvgKeysPerReprovide)
		require.Equal(t, 1, stats.Operations.Past.RegionReprovidedLastCycle)
		require.Equal(t, len(sollicitedPeers), stats.Network.Peers)
		require.Equal(t, len(reachablePeers), stats.Network.Reachable)
		require.False(t, stats.Network.CompleteKeyspaceCoverage)
		require.Equal(t, float64(len(reachablePeers))/float64(len(sollicitedPeers))*float64(replicationFactor), stats.Network.AvgHolders)
		require.Equal(t, replicationFactor, stats.Network.ReplicationFactor)

		keysPerPrefix := 3
		balancedKeys := make([]multihash.Multihash, len(b58KeysAllPrefixesBut00000))
		require.Len(t, b58KeysAllPrefixesBut00000, keysPerPrefix*(1<<avgPrefixLen-1))
		for i, k := range b58KeysAllPrefixesBut00000 {
			balancedKeys[i], err = multihash.FromB58String(k)
			require.NoError(t, err)
			// Test that the kadid of keys actually cover all prefixes
			bs := [32]byte{}
			bs[0] = byte(i/keysPerPrefix + 1) // +1 because we skip prefix "00000"
			b256 := bit256.NewKey(bs[:])
			require.True(t, keyspace.IsPrefix(bitstr.Key(key.BitString(b256)[bitsPerByte-avgPrefixLen:bitsPerByte]), keyspace.MhToBit256(balancedKeys[i])))
		}

		require.Equal(t, 1<<avgPrefixLen, len(balancedKeys)/keysPerPrefix+1) // sanity check
		nRegions := 1 << avgPrefixLen

		// Block upcoming provide operations
		blockedCond.L.Lock()
		blocked = true
		blockedCond.L.Unlock()

		// Advance time to 1 thick before mid cycle
		time.Sleep(time.Until(startTime.Add(reprovideInterval + reprovideInterval/2 - 1)))
		synctest.Wait()

		wg.Go(func() {
			err := prov.StartProviding(false, balancedKeys...)
			require.NoError(t, err)
		})

		synctest.Wait()

		stats = prov.Stats()

		concurrentProvides := maxWorkers - dedicatedPeriodicWorkers // all workers but the dedicated periodic workers

		require.False(t, stats.Closed)
		require.Equal(t, "online", stats.Connectivity.Status)
		require.Equal(t, startTime, stats.Connectivity.Since)
		require.Equal(t, len(balancedKeys)-keysPerPrefix*concurrentProvides, stats.Queues.PendingKeyProvides)    // exactly 3 keys per region
		require.Equal(t, len(balancedKeys)/keysPerPrefix-concurrentProvides, stats.Queues.PendingRegionProvides) // exactly 3 keys per region
		require.Equal(t, 0, stats.Queues.PendingRegionReprovides)
		require.Equal(t, reprovidedKeys+len(balancedKeys), stats.Schedule.Keys)
		require.Equal(t, nRegions, stats.Schedule.Regions)
		require.Equal(t, avgPrefixLen, stats.Schedule.AvgPrefixLength)
		require.Equal(t, bitstr.Key("10000"), stats.Schedule.NextReprovidePrefix) // prefix at "half" of the keyspace
		require.Equal(t, startTime.Add(reprovideInterval+prov.reprovideTimeForPrefix("10000")), stats.Schedule.NextReprovideAt)
		require.Equal(t, maxWorkers, stats.Workers.Max)
		require.Equal(t, concurrentProvides, stats.Workers.Active)
		require.Equal(t, concurrentProvides, stats.Workers.ActiveBurst)
		require.Equal(t, 0, stats.Workers.ActivePeriodic)
		require.Equal(t, dedicatedBurstWorkers, stats.Workers.DedicatedBurst)
		require.Equal(t, dedicatedPeriodicWorkers, stats.Workers.DedicatedPeriodic)
		require.Equal(t, 1, stats.Workers.QueuedBurst) // only 1 queued worker since they are created one by one
		require.Equal(t, 0, stats.Workers.QueuedPeriodic)
		require.Equal(t, maxConnsPerWorker, stats.Workers.MaxProvideConnsPerWorker)
		require.Equal(t, time.Since(startTime), stats.Timing.Uptime)
		require.Equal(t, reprovideInterval, stats.Timing.ReprovidesInterval)
		require.Equal(t, time.Since(startTime)%reprovideInterval, stats.Timing.CurrentTimeOffset)
		require.Equal(t, DefaultMaxReprovideDelay, stats.Timing.MaxReprovideDelay)
		require.Equal(t, concurrentProvides*keysPerPrefix, stats.Operations.Ongoing.KeyProvides) // 3 keys per region
		require.Equal(t, concurrentProvides, stats.Operations.Ongoing.RegionProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyReprovides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionReprovides)
		require.Equal(t, newKeysProvidedRecords+2*recordsProvided, stats.Operations.Past.RecordsProvided)
		require.Equal(t, reprovidedKeys+1, stats.Operations.Past.KeysProvided) // 1 provided, 6 reprovided
		require.Equal(t, 0, stats.Operations.Past.KeysFailed)
		require.Equal(t, 0., stats.Operations.Past.KeysProvidedPerMinute) // in the last cycle (last reprovideInterval), we didn't provide any new key
		require.Equal(t, float64(reprovidedKeys)/float64(reprovideDuration.Minutes()), stats.Operations.Past.KeysReprovidedPerMinute)
		require.Equal(t, reprovideDuration, stats.Operations.Past.RegionReprovideDuration)
		require.Equal(t, float64(reprovidedKeys), stats.Operations.Past.AvgKeysPerReprovide)
		require.Equal(t, 1, stats.Operations.Past.RegionReprovidedLastCycle)
		require.Equal(t, len(sollicitedPeers), stats.Network.Peers)
		require.Equal(t, len(reachablePeers), stats.Network.Reachable)
		require.False(t, stats.Network.CompleteKeyspaceCoverage)
		require.Equal(t, float64(len(reachablePeers))/float64(len(sollicitedPeers))*float64(replicationFactor), stats.Network.AvgHolders)
		require.Equal(t, replicationFactor, stats.Network.ReplicationFactor)

		blockedCond.L.Lock()
		blocked = false
		blockedCond.Broadcast()
		blockedCond.L.Unlock()

		wg.Wait()

		synctest.Wait()

		// Provide of all balanced keys is complete
		stats = prov.Stats()

		balancedKeysRecords := 0
		for _, k := range balancedKeys {
			closestPeers, err := router.GetClosestPeers(context.Background(), string(k))
			require.NoError(t, err)
			for _, p := range closestPeers {
				if peerReachability[p] {
					balancedKeysRecords++
				}
			}
		}

		require.False(t, stats.Closed)
		require.Equal(t, "online", stats.Connectivity.Status)
		require.Equal(t, startTime, stats.Connectivity.Since)
		require.Equal(t, 0, stats.Queues.PendingKeyProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionReprovides)
		require.Equal(t, reprovidedKeys+len(balancedKeys), stats.Schedule.Keys)
		require.Equal(t, nRegions, stats.Schedule.Regions)
		require.Equal(t, avgPrefixLen, stats.Schedule.AvgPrefixLength)
		require.Equal(t, bitstr.Key("10000"), stats.Schedule.NextReprovidePrefix) // prefix at "half" of the keyspace
		require.Equal(t, startTime.Add(reprovideInterval+prov.reprovideTimeForPrefix("10000")), stats.Schedule.NextReprovideAt)
		require.Equal(t, maxWorkers, stats.Workers.Max)
		require.Equal(t, 0, stats.Workers.Active)
		require.Equal(t, 0, stats.Workers.ActiveBurst)
		require.Equal(t, 0, stats.Workers.ActivePeriodic)
		require.Equal(t, dedicatedBurstWorkers, stats.Workers.DedicatedBurst)
		require.Equal(t, dedicatedPeriodicWorkers, stats.Workers.DedicatedPeriodic)
		require.Equal(t, 0, stats.Workers.QueuedBurst)
		require.Equal(t, 0, stats.Workers.QueuedPeriodic)
		require.Equal(t, maxConnsPerWorker, stats.Workers.MaxProvideConnsPerWorker)
		require.Equal(t, time.Since(startTime), stats.Timing.Uptime)
		require.Equal(t, reprovideInterval, stats.Timing.ReprovidesInterval)
		require.Equal(t, time.Since(startTime)%reprovideInterval, stats.Timing.CurrentTimeOffset)
		require.Equal(t, DefaultMaxReprovideDelay, stats.Timing.MaxReprovideDelay)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyReprovides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionReprovides)
		require.Equal(t, balancedKeysRecords+newKeysProvidedRecords+2*recordsProvided, stats.Operations.Past.RecordsProvided)
		require.Equal(t, len(balancedKeys)+reprovidedKeys+1, stats.Operations.Past.KeysProvided)
		require.Equal(t, 0, stats.Operations.Past.KeysFailed)
		require.Equal(t, 0., stats.Operations.Past.KeysProvidedPerMinute) // in the last cycle (last reprovideInterval), we didn't provide any new key
		require.Equal(t, float64(reprovidedKeys)/float64(reprovideDuration.Minutes()), stats.Operations.Past.KeysReprovidedPerMinute)
		require.Equal(t, reprovideDuration, stats.Operations.Past.RegionReprovideDuration)
		require.Equal(t, float64(reprovidedKeys), stats.Operations.Past.AvgKeysPerReprovide)
		require.Equal(t, 1, stats.Operations.Past.RegionReprovidedLastCycle)
		require.Equal(t, len(sollicitedPeers), stats.Network.Peers)
		require.Equal(t, len(reachablePeers), stats.Network.Reachable)
		require.False(t, stats.Network.CompleteKeyspaceCoverage)
		require.Equal(t, float64(balancedKeysRecords+newKeysProvidedRecords+recordsProvided)/float64(len(balancedKeys)+len(newKeys)+1), stats.Network.AvgHolders)
		require.Equal(t, replicationFactor, stats.Network.ReplicationFactor)

		// Wait a full reprovide cycle for all reprovide to happen
		time.Sleep(reprovideInterval)
		synctest.Wait()

		// Check stats after all regions have been reprovided
		stats = prov.Stats()

		require.False(t, stats.Closed)
		require.Equal(t, "online", stats.Connectivity.Status)
		require.Equal(t, startTime, stats.Connectivity.Since)
		require.Equal(t, 0, stats.Queues.PendingKeyProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionProvides)
		require.Equal(t, 0, stats.Queues.PendingRegionReprovides)
		require.Equal(t, reprovidedKeys+len(balancedKeys), stats.Schedule.Keys)
		require.Equal(t, nRegions, stats.Schedule.Regions)
		require.Equal(t, avgPrefixLen, stats.Schedule.AvgPrefixLength)
		require.Equal(t, bitstr.Key("10000"), stats.Schedule.NextReprovidePrefix) // prefix at "half" of the keyspace
		require.Equal(t, startTime.Add(2*reprovideInterval+prov.reprovideTimeForPrefix("10000")), stats.Schedule.NextReprovideAt)
		require.Equal(t, maxWorkers, stats.Workers.Max)
		require.Equal(t, 0, stats.Workers.Active)
		require.Equal(t, 0, stats.Workers.ActiveBurst)
		require.Equal(t, 0, stats.Workers.ActivePeriodic)
		require.Equal(t, dedicatedBurstWorkers, stats.Workers.DedicatedBurst)
		require.Equal(t, dedicatedPeriodicWorkers, stats.Workers.DedicatedPeriodic)
		require.Equal(t, 0, stats.Workers.QueuedBurst)
		require.Equal(t, 0, stats.Workers.QueuedPeriodic)
		require.Equal(t, maxConnsPerWorker, stats.Workers.MaxProvideConnsPerWorker)
		require.Equal(t, time.Since(startTime), stats.Timing.Uptime)
		require.Equal(t, reprovideInterval, stats.Timing.ReprovidesInterval)
		require.Equal(t, time.Since(startTime)%reprovideInterval, stats.Timing.CurrentTimeOffset)
		require.Equal(t, DefaultMaxReprovideDelay, stats.Timing.MaxReprovideDelay)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionProvides)
		require.Equal(t, 0, stats.Operations.Ongoing.KeyReprovides)
		require.Equal(t, 0, stats.Operations.Ongoing.RegionReprovides)
		require.Equal(t, 2*(balancedKeysRecords+newKeysProvidedRecords+recordsProvided)+recordsProvided, stats.Operations.Past.RecordsProvided)
		require.Equal(t, 2*(len(balancedKeys)+reprovidedKeys)+1, stats.Operations.Past.KeysProvided)
		require.Equal(t, 0, stats.Operations.Past.KeysFailed)
		require.Equal(t, 0., stats.Operations.Past.KeysProvidedPerMinute) // in the last cycle (last reprovideInterval), we didn't provide any new key
		require.Equal(t, 0., stats.Operations.Past.KeysReprovidedPerMinute)
		require.Equal(t, time.Duration(0), stats.Operations.Past.RegionReprovideDuration) // reprovides finish instantly since we don't block them
		require.Equal(t, (float64(keysPerPrefix)*float64(nRegions-1)+float64(reprovidedKeys))/float64(nRegions), stats.Operations.Past.AvgKeysPerReprovide)
		require.Equal(t, nRegions, stats.Operations.Past.RegionReprovidedLastCycle)
		require.Equal(t, len(peers), stats.Network.Peers)
		require.Equal(t, len(peers)*(reachabilityModulo-1)/reachabilityModulo, stats.Network.Reachable)
		require.True(t, stats.Network.CompleteKeyspaceCoverage) // After reproviding all regions, we cover the full keyspace
		require.Equal(t, float64(replicationFactor)*float64(reachabilityModulo-1)/float64(reachabilityModulo), stats.Network.AvgHolders)
		require.Equal(t, replicationFactor, stats.Network.ReplicationFactor)

		// Switch to disconnected
		router.getClosestPeersFunc = func(ctx context.Context, k string) ([]peer.ID, error) {
			return nil, errors.New("offline")
		}
		prov.connectivity.TriggerCheck()
		synctest.Wait()

		require.Equal(t, "disconnected", prov.Stats().Connectivity.Status)

		// Switch to offline
		time.Sleep(offlineDelay)
		synctest.Wait()

		require.Equal(t, "offline", prov.Stats().Connectivity.Status)

		prov.Close()
		require.True(t, prov.Stats().Closed)
	})
}

// Having a list of b58 encoded multihashes covering the required prefixes is
// more efficient than bruteforcing them when running the tests. Multihashes
// are verified to cover the expected regions during the tests, which is less
// resource intensive than generating them.
var b58KeysPrefix0000000 = []string{
	"QmUP6fJwYKziWDu9xiarXXhzhWa5MVBCWsokvovt456wF9", // "0000000"
	"QmTeM1BuLnMYugeAG2qWvwNNQiKJf9378PKZb84oy5oTkw", // "0000000"
	"QmQMAH3tzAfoTDBNCJaXakrsdFV5CkewkxzsjzM8d8pYF3", // "0000000"
	"QmPKoxYiiPrEJULVmMK23gV3W1R6ZXqm5yaE1uBqcqttKH", // "0000000"
	"QmTdQyRGASp4s5iwoF9fx6sLVK39vBQD8do2zBXNDRK4bT", // "0000000"

}

// covers all prefixes but "00000", with 3 keys for each prefix
var b58KeysAllPrefixesBut00000 = []string{
	"QmTi5wvUuSnzs1joycSfoowEviytE7vAcrsciruHhTmuDq", // "00001"
	"QmQtviPDckXVhdo9SYqjGJ6WgGRo9x94o3kxX5uGea2MYX", // "00001"
	"QmQjahrc1rqtdMctocDdJMTujdcYEpVPPczuZEZVogrm2K", // "00001"
	"QmcoguGWhyHeAW4pvVqTYGfvhZuGL7tApaGbcMBYMhNeJJ", // "00010"
	"QmXRDsfD8GuwYxbZ3NduLA6GdQ1B83y5b93ujXE5sEwm4J", // "00010"
	"QmNYn53ViKQtGMWzPPPt6qw9GzBTfBTcHTcMYnU6d1SECq", // "00010"
	"QmRKuYmrQu1oMLHDyiA2v66upmEB5JLRqVhVEYXYYM5agi", // "00011"
	"QmXqfL6XzJ22phYTH9JbdDu9REZzxazcjJsjiGHbj8p9oY", // "00011"
	"QmSsMn1Ld1uyVe8zEZS1hroH7LfRkein454Ct8wmPKpEu3", // "00011"
	"QmXBShDJEPyP4pjpS2XdqfCHy1Dsosxw8MBi9rjgjDCzi3", // "00100"
	"QmZ3oVg9BjVzZnthk4WEByjvPaQhCKMvMQirkiYSE36P9f", // "00100"
	"QmehqJAkVka6CSeq8QdYg84DA18QRfxtqs5PR6HGvdNSy2", // "00100"
	"QmbjGwSnsdBPvNocwv5tjFNVRzRg64GQGiS93BAPEohvkb", // "00101"
	"QmR49cDQ8TRoXHdquSyx8fN6NHxoXAUDaWtrQzfaRTfSba", // "00101"
	"QmTeCatDYBWSpSoqV3HFsrdS7U3SpmojywFnP6tt5irRqK", // "00101"
	"QmeiqM1WAaNGU5nqAsqzp2kVVcjZGYTfeo1kHktQ6LdcxT", // "00110"
	"QmZYCHCMLd9tohK3DcPG7QYi5R7m7aGyFuA7SJE7T3seGS", // "00110"
	"QmSA4UqdLxZke4RtZh41Bxzk5p62GGEHfhhFWtcoeDiXXy", // "00110"
	"QmWV2B5h6esbJZXyksPyb9bxrHYzkeW8HVYit8vJbkk6Pz", // "00111"
	"QmcSczgDZCzC36Ktswx7KTESfpn6NV3n4j2i2CTB4CgaVC", // "00111"
	"QmT4R6x3a5oHoxMmZ329BYmZSNBJBvLd1q8wcCYAE7xdZS", // "00111"
	"QmYJPtDLaCqKUDBYUKWhKvPyckgWubXPKoAH3kth351YcW", // "01000"
	"QmXJS1PUTdmRcxmDawUCBV6QdHV6NMxzDoFD44TULssSZE", // "01000"
	"QmWSXsLMqSKBT4msxvkNrAivELvfvXmE3LQ9xzSoNBAyuz", // "01000"
	"QmQ3aqKBSY5r8k1CZv7FPhcPEjNG47yzSuzKpfUUgJXdB4", // "01001"
	"QmPWAhCbmbNsEwnDcLzNdiqZwS8bkGSKUbJr4YSaeUeN2w", // "01001"
	"QmWKnmrbetcwmTvppphGJc3oe6HD47a6SUEpAUscexYzrc", // "01001"
	"QmXvbB3L7uHTNhTLDtSfB7YDFo6dTZsTK2PZuSyZ6MBLET", // "01010"
	"QmWKMjESS6QszdbJ8BYenkbLJN14Wk5Y3ib4uo2wd5ifbs", // "01010"
	"Qmbye4JcvN1qXZQpCQNCwawhEXwvn2GT7vrk2ttrjqJ3wu", // "01010"
	"QmVGpQGZWCJLVREtuwsUfcpt7y4bnH3tpaQQ4gHiRyLPXL", // "01011"
	"Qma6TEBZp2drjd9yGSPU27kd4wRLce1G2qGNwZrL3E88qm", // "01011"
	"QmXvSYMBHCPvPEeUxBn8Nif3iqdF5FNLgjpAJzWBFwznHC", // "01011"
	"QmXkMYAnv2xWtixhzJqUdvUMTUymLtvkZbpZHLpjv8RkQU", // "01100"
	"QmS33LkDXN1mkuFovVfqYvmDjEoWr2MXxgKWokHNXeFP4g", // "01100"
	"QmeSJ1cekSdxgzmJDnH7nyJu3HCjsU4nKCxNkh5wABE9HN", // "01100"
	"QmPQSU7aBt7czsyHJCNPcrU161aPugYEjU3FjAG5SiKiQY", // "01101"
	"QmSdveHM1ShmHEjrNyLdkY9MYPyEKCgVvWXHvvbXCTP951", // "01101"
	"QmX2a7HNhRzp4eEdYGhyzArKLqySZJgHmEP4NUL8wSA4qh", // "01101"
	"QmeAuUnQh6MsvEwvUYfrFMGb5dfdU1UvuYq5yYQjsFoBmA", // "01110"
	"QmaHXADi79HCmmGPYMmdqvyemChRmZPVGyEQYmo6oS2C3a", // "01110"
	"QmfWkHicRFPqsiWyfGBhH2cm6jwKkhWm34Jr6P4W4b7PqV", // "01110"
	"QmaeCb1Ukgj6Ep6vwkwT6RcMpuBjsKLje6scJZ1dPSSZye", // "01111"
	"QmfXSMwFCsrY2H93BsZAwPjj8uPV9jLKdgk4fWMDfnM4Xb", // "01111"
	"QmRuqS9hv17XT752DBKinP5YxqKSVVtmv9huQEKsch75qR", // "01111"
	"QmTCkjcttVzYqFbnEnDm81xEVv8hxHDcS98P6rbFXNJgFV", // "10000"
	"QmTLZn8wTHzQ4Vh9vxi68uN4HiR7EB26ro6paNMP8k61GQ", // "10000"
	"QmW1PWSRubC9BoNzyTovCibfDSRnVwSEN9t6JdxVVkVD5o", // "10000"
	"Qma15oSH3ng2EnPdtugUKELEmabdWT8bU3tDcz3fjLNvuu", // "10001"
	"QmYSHGyag9VexAPdDUkXtbqkQfS4Htw5iy1tUAgKSbdEmy", // "10001"
	"Qmd8FJdsaA45WmzPsTeTt6MraabkJ3F9bVHCWLqNAK5JXf", // "10001"
	"QmNnoKtte57JBNh5jMiCcGXiV3RvC1ea6HekvZn7RebPEQ", // "10010"
	"QmUPWWPbGb8rpr8R9MsA8SWYuT39ADZfdNCKehoGa1BNGH", // "10010"
	"QmZFQH8SJ9y8jyUzbDmLswu9Kow7QZaiA3AoSpCYf6xycA", // "10010"
	"QmeATEHx9nRkCU6uCUXPnUfnK3AKLBTMpbK1qQs4FVPdi6", // "10011"
	"QmTbEvXabndtyFm6zYZBvYg82jy9Po9bX8dXbyPAEWYh1f", // "10011"
	"QmSmsrYVdbdVcVo4YfH7jVLVdn565fCrfjNzQ5a1qZQ677", // "10011"
	"QmYa1c3FhLiAv1q7brUHqES6pozn3ud7pzpfWo5hizYMex", // "10100"
	"QmUiYH84v6XPhXQP6hmPm1UoWKvgWoKntzvsJ7VsRH8cVM", // "10100"
	"QmbksqVSesaAj9y8AYvoL11xghqncEdrm2sC6kaJE3LhrK", // "10100"
	"QmX77YWWarMCDrudRq4EGxbeTJUNqkKwQHg1Qhk12TutR2", // "10101"
	"Qmd2q8g8bsxc2jhF4z1ncxsLDKEouszY6617pWBLUhADbq", // "10101"
	"QmdbFMamPX7nA2XdCCuuCpd2j6MguNgYcUcbNL634Trfuc", // "10101"
	"QmbQm1W47hgHvo2Ytj1DNKVymc6KsXvPYW5AQxvi85LcGr", // "10110"
	"QmRc6j9R45L2KCKAJdc8dU4opdzR3H5puJrzaPYkLmw7et", // "10110"
	"QmaGw2k1MbGqRnEf7PBVHohGLS4U8cKaRR5yyYtuY71MhD", // "10110"
	"QmSTaQCW56uyTc2ZDdJSh1zC57jAYJTs5WmgyMBKJy9Y6B", // "10111"
	"QmciXzw84G1cBdeNy6BGEp8vsCWDi36XW4TQy3jZtsvFHc", // "10111"
	"QmexMfmQYGUiJVFm1YNgx1rqMsfgoz7zVc9V7ZeT1Ri3oK", // "10111"
	"QmZgZfjpfh1j5GJL1dXTYxQjKRKRwrnGuHu9ied7CcCv8a", // "11000"
	"QmVq6eUjrUHckcUEbJF6pSdfWAAsFwi57jtdm5Sn5VFvRp", // "11000"
	"QmSy9KniQxHKGgRXmCwdnFsLB34yye6DYCf8s573ueX1Be", // "11000"
	"QmZYmYZ3NEy1pqgKw5XNe4GGZ7XPLNWUGdYtLA79b8vqLN", // "11001"
	"QmPdENpCT9hb5XUmcGnSwjC6H4zv1DzsiFX1iTJ1eHm3CQ", // "11001"
	"QmVaPTddRyjLjMoZnYufWc5M5CjyGNPmFEpp5HtPKEqZFG", // "11001"
	"Qmb7eDkyt4gFqFUZxX5ETR8NeqEktxG2BLrHpbkSKqeek9", // "11010"
	"Qmdx97coS2FKH2Wc1sr3fUnrw3ooicyfBcaEKQx9vdiGU2", // "11010"
	"QmeUvSAHr3WT4U1L2woYCQPFWfKkJ9WdDUQtWDan7XzAEy", // "11010"
	"QmZT2f1Gr4BdfRw87JdkrPRt5sFGNEbjpbxwSH6qX57FLC", // "11011"
	"Qmet7SbXQGwuCueLukMgk6tdZkeoKqNGr4yvHpF1CkuDyU", // "11011"
	"QmR2KwJg98djLrAwzJiV7ezL2K8NCG1Bf6GFu6oHxT1dNC", // "11011"
	"QmaRy3XbCRYu1kPLJAM8y9BkxfsXTuRLH159RpZyyeaqMH", // "11100"
	"QmPJqs7h3kmh7geuaUoYaRK51uZgZASuA5x88vg9aDC4VD", // "11100"
	"QmPHfWnxvUuQcSaaktazFv8iZMYddu1MSDVthdxS247bg2", // "11100"
	"QmcWoTybhP6KWRT5mibX2M3m5SL9LMgJd7WXXdVy6n7dfN", // "11101"
	"QmaySv2xLytecaYXBqdEGSAVbYHhQRbqmWx7SmFUGeRFL8", // "11101"
	"QmcMEdpubx1VyaDgK5VMdu6qqw9QJkDPJDiQaGewLGiVqM", // "11101"
	"QmWdBzEQvbw7A2GRtq52fAj9Ad3J5A18vAYqvvuVpQ8v7D", // "11110"
	"QmTPwmaQtBLq9RXbvNyfj46X65YShYzMzn62FFbNYcieEm", // "11110"
	"QmZB1teMQMK9PNBtuvot1rE4N5UZnyj3vSMT9zBfCHYBV2", // "11110"
	"QmXSA9wTmhvJQq4psyfgdNXHGWvJ6KnGcbHh7oaxKYE4zH", // "11111"
	"QmWtZSzMVxUdbSQUSkCBdsyypsZguZoULjZiX7fVCp8EG4", // "11111"
	"QmTvzhm9ueCbgTFxA8RYT5vbuGkbYcSyXRct1kVnrTerJk", // "11111"
}
