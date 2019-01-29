package dht

import (
	"context"
	"sync"
	"testing"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"
	queue "github.com/libp2p/go-libp2p-peerstore/queue"
)

func init() {
	DialQueueScalingMutePeriod = 0
	DialQueueMaxIdle = 1 * time.Second
}

func TestDialQueueErrorsWithTooManyConsumers(t *testing.T) {
	var calls int
	defer func() {
		if e := recover(); e == nil {
			t.Error("expected a panic, got none")
		} else if calls != 4 {
			t.Errorf("expected a panic on the 4th call to Consume(); got it on call number %d", calls)
		}
	}()

	in := queue.NewChanQueue(context.Background(), queue.NewXORDistancePQ("test"))
	hang := make(chan struct{})
	dialFn := func(ctx context.Context, p peer.ID) error {
		<-hang
		return nil
	}

	dq := newDialQueue(context.Background(), "test", in, dialFn, 3)
	for ; calls < 3; calls++ {
		dq.Consume()
	}
	calls++
	dq.Consume()
}

func TestDialQueueGrowsOnSlowDials(t *testing.T) {
	in := queue.NewChanQueue(context.Background(), queue.NewXORDistancePQ("test"))
	hang := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(19) // we expect 19 workers
	dialFn := func(ctx context.Context, p peer.ID) error {
		wg.Done()
		<-hang
		return nil
	}

	// Enqueue 20 jobs.
	for i := 0; i < 20; i++ {
		in.EnqChan <- peer.ID(i)
	}

	// remove the mute period to grow faster.
	dq := newDialQueue(context.Background(), "test", in, dialFn, 4)

	for i := 0; i < 4; i++ {
		_ = dq.Consume()
		time.Sleep(100 * time.Millisecond)
	}

	doneCh := make(chan struct{})

	// wait in a goroutine in case the test fails and we block.
	go func() {
		defer close(doneCh)
		wg.Wait()
	}()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Error("expected 19 concurrent dials, got less")
	}
}

func TestDialQueueShrinksWithNoConsumers(t *testing.T) {
	in := queue.NewChanQueue(context.Background(), queue.NewXORDistancePQ("test"))
	hang := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(13)
	dialFn := func(ctx context.Context, p peer.ID) error {
		wg.Done()
		<-hang
		return nil
	}

	dq := newDialQueue(context.Background(), "test", in, dialFn, 3)

	// Enqueue 13 jobs, one per worker we'll grow to.
	for i := 0; i < 13; i++ {
		in.EnqChan <- peer.ID(i)
	}

	// acquire 3 consumers, everytime we acquire a consumer, we will grow the pool because no dial job is completed
	// and immediately returnable.
	for i := 0; i < 3; i++ {
		_ = dq.Consume()
		time.Sleep(100 * time.Millisecond)
	}

	waitForWg(t, &wg, 2*time.Second)

	// Release a few dialFn, but not all of them because downscaling happens when workers detect there are no
	// consumers to consume their values. So the other three will be these witnesses.
	for i := 0; i < 10; i++ {
		hang <- struct{}{}
	}

	// allow enough time for signalling and dispatching values to outstanding consumers.
	time.Sleep(500 * time.Millisecond)

	// unblock the other three.
	hang <- struct{}{}
	hang <- struct{}{}
	hang <- struct{}{}

	// we should now only have 6 workers, because all the shrink events will have been honoured.
	wg.Add(6)

	// enqueue more jobs
	for i := 0; i < 20; i++ {
		in.EnqChan <- peer.ID(i)
	}

	// let's check we have 6 workers hanging.
	waitForWg(t, &wg, 2*time.Second)
}

// Inactivity = workers are idle because the DHT query is progressing slow and is producing too few peers to dial.
func TestDialQueueShrinksWithInactivity(t *testing.T) {
	in := queue.NewChanQueue(context.Background(), queue.NewXORDistancePQ("test"))
	hang := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(13)
	dialFn := func(ctx context.Context, p peer.ID) error {
		wg.Done()
		<-hang
		return nil
	}

	// Enqueue 13 jobs.
	for i := 0; i < 13; i++ {
		in.EnqChan <- peer.ID(i)
	}

	dq := newDialQueue(context.Background(), "test", in, dialFn, 3)

	// keep up to speed with backlog by releasing the dial function every time we acquire a channel.
	for i := 0; i < 13; i++ {
		ch := dq.Consume()
		hang <- struct{}{}
		<-ch
		time.Sleep(100 * time.Millisecond)
	}

	// wait for MaxIdlePeriod.
	time.Sleep(1500 * time.Millisecond)

	// we should now only have 6 workers, because all the shrink events will have been honoured.
	wg.Add(6)

	// enqueue more jobs
	for i := 0; i < 10; i++ {
		in.EnqChan <- peer.ID(i)
	}

	// let's check we have 6 workers hanging.
	waitForWg(t, &wg, 2*time.Second)
}

func TestDialQueueMutePeriodHonored(t *testing.T) {
	DialQueueScalingMutePeriod = 2 * time.Second

	in := queue.NewChanQueue(context.Background(), queue.NewXORDistancePQ("test"))
	hang := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(6)
	dialFn := func(ctx context.Context, p peer.ID) error {
		wg.Done()
		<-hang
		return nil
	}

	// Enqueue a bunch of jobs.
	for i := 0; i < 20; i++ {
		in.EnqChan <- peer.ID(i)
	}

	dq := newDialQueue(context.Background(), "test", in, dialFn, 3)

	// pick up three consumers.
	for i := 0; i < 3; i++ {
		_ = dq.Consume()
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)

	// we'll only have 6 workers because the grow signals have been ignored.
	waitForWg(t, &wg, 2*time.Second)
}

func waitForWg(t *testing.T, wg *sync.WaitGroup, wait time.Duration) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-time.After(wait):
		t.Error("timeout while waiting for WaitGroup")
	case <-done:
	}
}
