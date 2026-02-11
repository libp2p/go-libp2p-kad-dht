package buffered

import (
	"bytes"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal"
	mh "github.com/multiformats/go-multihash"
)

var _ internal.Provider = (*fakeProvider)(nil)

type fakeProvider struct {
	mu                  sync.Mutex
	provideOnceCalls    [][]mh.Multihash
	startProvidingCalls []startProvidingCall
	stopProvidingCalls  [][]mh.Multihash

	// Signal when operations are processed
	processed chan struct{}
}

type startProvidingCall struct {
	force bool
	keys  []mh.Multihash
}

func (f *fakeProvider) ProvideOnce(keys ...mh.Multihash) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(keys) > 0 {
		f.provideOnceCalls = append(f.provideOnceCalls, keys)
		if f.processed != nil {
			select {
			case f.processed <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

func (f *fakeProvider) StartProviding(force bool, keys ...mh.Multihash) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(keys) > 0 {
		f.startProvidingCalls = append(f.startProvidingCalls, startProvidingCall{
			force: force,
			keys:  keys,
		})
		if f.processed != nil {
			select {
			case f.processed <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

func (f *fakeProvider) StopProviding(keys ...mh.Multihash) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(keys) > 0 {
		f.stopProvidingCalls = append(f.stopProvidingCalls, keys)
		if f.processed != nil {
			select {
			case f.processed <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

func (f *fakeProvider) Clear() int {
	// Unused
	return 0
}

func (f *fakeProvider) RefreshSchedule() error {
	// Unused
	return nil
}

func (f *fakeProvider) Close() error {
	// Unused
	return nil
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{
		processed: make(chan struct{}, 10), // Buffered channel for test signaling
	}
}

func TestQueueingMechanism(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newFakeProvider()
		ds := datastore.NewMapDatastore()
		provider := New(fake, ds,
			WithDsName("test1"),
			WithIdleWriteTime(time.Millisecond),
			WithBatchSize(10))
		defer provider.Close()

		keys := random.Multihashes(3)

		// Queue various operations
		if err := provider.ProvideOnce(keys[0]); err != nil {
			t.Fatalf("ProvideOnce failed: %v", err)
		}
		if err := provider.StartProviding(false, keys[1]); err != nil {
			t.Fatalf("StartProviding failed: %v", err)
		}
		if err := provider.StartProviding(true, keys[2]); err != nil {
			t.Fatalf("StartProviding (force) failed: %v", err)
		}
		if err := provider.StopProviding(keys[0]); err != nil {
			t.Fatalf("StopProviding failed: %v", err)
		}

		// Wait for operations to be processed by expecting 4 signals
		for i := range 4 {
			select {
			case <-fake.processed:
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for operation %d to be processed", i+1)
			}
		}

		// Verify all operations were dequeued and processed
		if len(fake.provideOnceCalls) != 1 {
			t.Errorf("Expected 1 ProvideOnce call, got %d", len(fake.provideOnceCalls))
		} else if len(fake.provideOnceCalls[0]) != 1 || !bytes.Equal(fake.provideOnceCalls[0][0], keys[0]) {
			t.Errorf("Expected ProvideOnce call with keys[0], got %v", fake.provideOnceCalls[0])
		}

		if len(fake.startProvidingCalls) != 2 {
			t.Errorf("Expected 2 StartProviding calls, got %d", len(fake.startProvidingCalls))
		} else {
			// Check that we have one force=true call and one force=false call
			foundForce := false
			foundRegular := false
			for _, call := range fake.startProvidingCalls {
				if call.force {
					foundForce = true
					if len(call.keys) != 1 || !bytes.Equal(call.keys[0], keys[2]) {
						t.Errorf("Expected force StartProviding call with keys[2], got %v", call.keys)
					}
				} else {
					foundRegular = true
					if len(call.keys) != 1 || !bytes.Equal(call.keys[0], keys[1]) {
						t.Errorf("Expected regular StartProviding call with keys[1], got %v", call.keys)
					}
				}
			}
			if !foundForce {
				t.Errorf("Expected to find a StartProviding call with force=true")
			}
			if !foundRegular {
				t.Errorf("Expected to find a StartProviding call with force=false")
			}
		}

		if len(fake.stopProvidingCalls) != 1 {
			t.Errorf("Expected 1 StopProviding call, got %d", len(fake.stopProvidingCalls))
		} else if len(fake.stopProvidingCalls[0]) != 1 || !bytes.Equal(fake.stopProvidingCalls[0][0], keys[0]) {
			t.Errorf("Expected StopProviding call with keys[0], got %v", fake.stopProvidingCalls[0])
		}
	})
}

func TestStartProvidingAfterStopProvidingRemovesStopOperation(t *testing.T) {
	// Test the core logic directly by calling getOperations with known data
	t.Run("DirectTest", func(t *testing.T) {
		key := random.Multihashes(1)[0]

		// Create batch data that simulates StopProviding followed by StartProviding
		stopData := toBytes(stopProvidingOp, key)
		startData := toBytes(startProvidingOp, key)

		dequeued := [][]byte{stopData, startData}
		ops, err := getOperations(dequeued) // We need to create this helper
		if err != nil {
			t.Fatalf("getOperations failed: %v", err)
		}

		// StartProviding should be present
		if len(ops[startProvidingOp]) != 1 || !bytes.Equal(ops[startProvidingOp][0], key) {
			t.Errorf("Expected StartProviding operation with key, got %v", ops[startProvidingOp])
		}

		// StopProviding should be canceled (empty)
		if len(ops[stopProvidingOp]) != 0 {
			t.Errorf("Expected StopProviding operations to be canceled, got %v", ops[stopProvidingOp])
		}
	})
}

func TestMultipleOperationsOnSameKey(t *testing.T) {
	// Test the core batch processing logic directly
	t.Run("DirectTest", func(t *testing.T) {
		key := random.Multihashes(1)[0]

		// Create batch data with multiple operations on same key
		ops := [][]byte{
			toBytes(stopProvidingOp, key),       // StopProviding
			toBytes(forceStartProvidingOp, key), // StartProviding(force=true)
			toBytes(stopProvidingOp, key),       // StopProviding again
			toBytes(startProvidingOp, key),      // StartProviding(force=false)
		}

		processed, err := getOperations(ops)
		if err != nil {
			t.Fatalf("getOperations failed: %v", err)
		}

		// Should have 2 StartProviding operations
		if len(processed[startProvidingOp]) != 1 {
			t.Errorf("Expected 1 StartProviding (force=false) operation, got %d", len(processed[startProvidingOp]))
		}
		if len(processed[forceStartProvidingOp]) != 1 {
			t.Errorf("Expected 1 StartProviding (force=true) operation, got %d", len(processed[forceStartProvidingOp]))
		}

		// StopProviding should be canceled (empty) because StartProviding operations were in same batch
		if len(processed[stopProvidingOp]) != 0 {
			t.Errorf("Expected 0 StopProviding operations (should be canceled), got %d", len(processed[stopProvidingOp]))
		}
	})
}

func TestBatchProcessing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newFakeProvider()
		ds := datastore.NewMapDatastore()
		provider := New(fake, ds,
			WithDsName("test4"),
			WithBatchSize(3), // Process 3 operations at once
			WithIdleWriteTime(time.Second))
		defer provider.Close()

		// Queue multiple keys - total of 3 operations (2 from ProvideOnce + 1 from StartProviding)
		keys := random.Multihashes(3)

		if err := provider.ProvideOnce(keys[0], keys[1]); err != nil {
			t.Fatalf("ProvideOnce failed: %v", err)
		}
		if err := provider.StartProviding(false, keys[2]); err != nil {
			t.Fatalf("StartProviding failed: %v", err)
		}
		synctest.Wait()

		// Close to ensure all operations are flushed
		provider.Close()

		// Verify operations were batched correctly
		totalProvideOnceCalls := 0
		for _, call := range fake.provideOnceCalls {
			totalProvideOnceCalls += len(call)
		}
		if totalProvideOnceCalls != 2 {
			t.Errorf("Expected 2 total keys in ProvideOnce calls, got %d", totalProvideOnceCalls)
		}

		totalStartProvidingCalls := 0
		for _, call := range fake.startProvidingCalls {
			totalStartProvidingCalls += len(call.keys)
		}
		if totalStartProvidingCalls != 1 {
			t.Errorf("Expected 1 total key in StartProviding calls, got %d", totalStartProvidingCalls)
		}
	})
}
