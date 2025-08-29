package datastore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger4"
	leveldb "github.com/ipfs/go-ds-leveldb"
	pebbleds "github.com/ipfs/go-ds-pebble"
	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal/keyspace"
	mh "github.com/multiformats/go-multihash"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

// createMapDatastore creates a simple in-memory map datastore
func createMapDatastore() (ds.Batching, func()) {
	s := ds.NewMapDatastore()
	return s, func() { s.Close() }
}

// createBadgerDatastore creates a Badger datastore
func createBadgerDatastore(b *testing.B) (ds.Batching, func()) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("badger-bench-%d", b.N))
	ds, err := badger.NewDatastore(dir, nil)
	if err != nil {
		b.Fatalf("Failed to create Badger datastore: %v", err)
	}
	return ds, func() {
		ds.Close()
		os.RemoveAll(dir)
	}
}

// createLevelDBDatastore creates a LevelDB datastore
func createLevelDBDatastore(b *testing.B) (ds.Batching, func()) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("leveldb-bench-%d", b.N))
	ds, err := leveldb.NewDatastore(dir, nil)
	if err != nil {
		b.Fatalf("Failed to create LevelDB datastore: %v", err)
	}
	return ds, func() {
		ds.Close()
		os.RemoveAll(dir)
	}
}

// createPebbleDatastore creates a Pebble datastore
func createPebbleDatastore(b *testing.B) (ds.Batching, func()) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("pebble-bench-%d", b.N))
	ds, err := pebbleds.NewDatastore(dir)
	if err != nil {
		b.Fatalf("Failed to create Pebble datastore: %v", err)
	}
	return ds, func() {
		ds.Close()
		os.RemoveAll(dir)
	}
}

type keystore interface {
	Put(context.Context, ...mh.Multihash) ([]mh.Multihash, error)
	Get(context.Context, bitstr.Key) ([]mh.Multihash, error)
	ContainsPrefix(context.Context, bitstr.Key) (bool, error)
	ResetCids(context.Context, <-chan cid.Cid) error
	Delete(context.Context, ...mh.Multihash) error
	Empty(context.Context) error
	Close() error
}

// Benchmark KeyStore Put operations
func BenchmarkKeyStorePut(b *testing.B) {
	testSizes := []int{100000}

	for _, size := range testSizes {
		// b.Run(fmt.Sprintf("Map_%d_keys", size), func(b *testing.B) {
		// 	s, cleanup := createMapDatastore()
		// 	defer cleanup()
		// 	benchmarkKeyStorePut(b, s, size)
		// })
		//
		// b.Run(fmt.Sprintf("Badger_%d_keys", size), func(b *testing.B) {
		// 	s, cleanup := createBadgerDatastore(b)
		// 	defer cleanup()
		// 	benchmarkKeyStorePut(b, s, size)
		// })

		b.Run(fmt.Sprintf("LevelDB_%d_keys", size), func(b *testing.B) {
			s, cleanup := createLevelDBDatastore(b)
			defer cleanup()
			benchmarkKeyStorePut(b, s, size)
		})

		b.Run(fmt.Sprintf("Pebble_%d_keys", size), func(b *testing.B) {
			s, cleanup := createPebbleDatastore(b)
			defer cleanup()
			benchmarkKeyStorePut(b, s, size)
		})
	}
}

func getKeyStores(s ds.Batching) map[string]keystore {
	ks1, err := NewKeyStore(s)
	if err != nil {
		panic("Failed to create KeyStore")
	}
	ks2, err := NewKeyStore2(s)
	if err != nil {
		panic("Failed to create KeyStore2")
	}
	ks3, err := NewKeyStore3(s)
	if err != nil {
		panic("Failed to create KeyStore3")
	}
	return map[string]keystore{
		"KeyStore":  ks1,
		"KeyStore2": ks2,
		"KeyStore3": ks3,
	}
}

func benchmarkKeyStorePut(b *testing.B, ds ds.Batching, keyCount int) {
	keystores := getKeyStores(ds)
	defer func() {
		for _, ks := range keystores {
			ks.Close()
		}
	}()

	nPuts := 1
	randomKeys := random.Multihashes(keyCount * nPuts)

	for str, ks := range keystores {
		b.Run(str, func(b *testing.B) {
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				// Reset the keystore for each iteration
				ks.Empty(ctx)

				for i := range nPuts {
					keys := randomKeys[i*keyCount : (i+1)*keyCount]
					_, err := ks.Put(ctx, keys...)
					if err != nil {
						b.Fatalf("Put failed: %v", err)
					}
				}
			}
		})
	}
}

// Benchmark KeyStore Get operations
func BenchmarkKeyStoreGet(b *testing.B) {
	testSizes := []int{10000}
	iterations := 1

	for _, size := range testSizes {
		b.Run(fmt.Sprintf("Map_%d_keys", size), func(b *testing.B) {
			s, cleanup := createMapDatastore()
			defer cleanup()
			benchmarkKeyStoreGet(b, s, size, iterations)
		})

		b.Run(fmt.Sprintf("Badger_%d_keys", size), func(b *testing.B) {
			s, cleanup := createBadgerDatastore(b)
			defer cleanup()
			benchmarkKeyStoreGet(b, s, size, iterations)
		})

		b.Run(fmt.Sprintf("LevelDB_%d_keys", size), func(b *testing.B) {
			s, cleanup := createLevelDBDatastore(b)
			defer cleanup()
			benchmarkKeyStoreGet(b, s, size, iterations)
		})

		b.Run(fmt.Sprintf("Pebble_%d_keys", size), func(b *testing.B) {
			s, cleanup := createPebbleDatastore(b)
			defer cleanup()
			benchmarkKeyStoreGet(b, s, size, iterations)
		})
	}
}

func generatePrefixes() []bitstr.Key {
	out := keyspace.ExtendBinaryPrefix("", 4)                     // 16 prefixes of len 4
	out = append(out, keyspace.ExtendBinaryPrefix("", 8)...)      // 256 prefixes of len 8
	out = append(out, keyspace.ExtendBinaryPrefix("1010", 12)...) // 256 prefixes of len 12
	return out
}

func benchmarkKeyStoreGet(b *testing.B, ds ds.Batching, keys, gets int) {
	keystores := getKeyStores(ds)
	defer func() {
		for _, ks := range keystores {
			ks.Close()
		}
	}()

	// Pre-populate the keystore
	randomKeys := random.Multihashes(keys)
	prefixes := generatePrefixes()

	for str, ks := range keystores {
		b.Run(str, func(b *testing.B) {
			ctx := context.Background()
			_, err := ks.Put(ctx, randomKeys...) // Pre-populate
			if err != nil {
				b.Fatalf("Put failed: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				// Test GET
				for range gets {
					for _, p := range prefixes {
						_, err := ks.Get(ctx, p)
						if err != nil {
							b.Fatalf("Get failed: %v", err)
						}
					}
				}
			}
		})
	}
}

// Benchmark KeyStore ContainsPrefix operations
func BenchmarkKeyStoreContainsPrefix(b *testing.B) {
	testSizes := []int{10000}
	iterations := 1

	for _, size := range testSizes {
		b.Run(fmt.Sprintf("Map_%d_keys", size), func(b *testing.B) {
			s, cleanup := createMapDatastore()
			defer cleanup()
			benchmarkKeyStoreContainsPrefix(b, s, size, iterations)
		})

		b.Run(fmt.Sprintf("Badger_%d_keys", size), func(b *testing.B) {
			s, cleanup := createBadgerDatastore(b)
			defer cleanup()
			benchmarkKeyStoreContainsPrefix(b, s, size, iterations)
		})

		b.Run(fmt.Sprintf("LevelDB_%d_keys", size), func(b *testing.B) {
			s, cleanup := createLevelDBDatastore(b)
			defer cleanup()
			benchmarkKeyStoreContainsPrefix(b, s, size, iterations)
		})

		b.Run(fmt.Sprintf("Pebble_%d_keys", size), func(b *testing.B) {
			s, cleanup := createPebbleDatastore(b)
			defer cleanup()
			benchmarkKeyStoreContainsPrefix(b, s, size, iterations)
		})
	}
}

func benchmarkKeyStoreContainsPrefix(b *testing.B, ds ds.Batching, keys, ops int) {
	keystores := getKeyStores(ds)
	defer func() {
		for _, ks := range keystores {
			ks.Close()
		}
	}()

	// Pre-populate the keystore
	randomKeys := random.Multihashes(keys)
	prefixes := generatePrefixes()

	for str, ks := range keystores {
		b.Run(str, func(b *testing.B) {
			ctx := context.Background()
			_, err := ks.Put(ctx, randomKeys...) // Pre-populate
			if err != nil {
				b.Fatalf("Put failed: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				// Test COntainsPrefix
				for range ops {
					for _, p := range prefixes {
						_, err := ks.ContainsPrefix(ctx, p)
						if err != nil {
							b.Fatalf("COntainsPrefix failed: %v", err)
						}
					}
				}
			}
		})
	}
}

// func benchmarkKeyStoreAllOps(b *testing.B, ds ds.Batching, put int) {
// 	keystores := getKeyStores(ds)
// 	defer func() {
// 		for _, ks := range keystores {
// 			ks.Close()
// 		}
// 	}()
//
// 	// Pre-populate the keystore
// 	putBatches := 10
// 	randomKeys := random.Multihashes(put)
// 	prefixes := generatePrefixes()
//
// 	for str, ks := range keystores {
// 		b.Run(str, func(b *testing.B) {
// 			ctx := context.Background()
//
// 			b.ResetTimer()
// 			b.ReportAllocs()
//
// 			for b.Loop() {
// 				// Reset the keystore for each iteration
// 				ks.Empty(ctx)
//
// 				// Test PUT
// 				for i := range putBatches {
// 					keys := randomKeys[i*(put/putBatches) : (i+1)*put/putBatches]
// 					_, err := ks.Put(ctx, keys...)
// 					if err != nil {
// 						b.Fatalf("Put failed: %v", err)
// 					}
// 				}
//
// 				// Test GET
// 				for _, p := range prefixes {
// 					_, err := ks.Get(ctx, p)
// 					if err != nil {
// 						b.Fatalf("Get failed: %v", err)
// 					}
// 				}
//
// 				// Test ContainsPrefix
// 				for _, p := range prefixes {
// 					_, err := ks.ContainsPrefix(ctx, p)
// 					if err != nil {
// 						b.Fatalf("ContainsPrefix failed: %v", err)
// 					}
// 				}
// 			}
// 		})
// 	}
// }
