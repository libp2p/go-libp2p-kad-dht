package dht

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"
	queue "github.com/libp2p/go-libp2p-peerstore/queue"
)

func init() {
	DialQueueScalingMutePeriod = 0
}

func TestDialQueueGrowsOnSlowDials(t *testing.T) {
	DialQueueMaxIdle = 10 * time.Minute

	in := queue.NewChanQueue(context.Background(), queue.NewXORDistancePQ("test"))
	hang := make(chan struct{})

	var cnt int32
	dialFn := func(ctx context.Context, p peer.ID) error {
		atomic.AddInt32(&cnt, 1)
		<-hang
		return nil
	}

	// Enqueue 20 jobs.
	for i := 0; i < 20; i++ {
		in.EnqChan <- peer.ID(i)
	}

	// remove the mute period to grow faster.
	dq := newDialQueue(context.Background(), "test", in, dialFn)

	for i := 0; i < 4; i++ {
		_ = dq.Consume()
		time.Sleep(100 * time.Millisecond)
	}

	for i := 0; i < 20; i++ {
		if atomic.LoadInt32(&cnt) > int32(DialQueueMinParallelism) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Errorf("expected 19 concurrent dials, got %d", atomic.LoadInt32(&cnt))

}

func TestDialQueueShrinksWithNoConsumers(t *testing.T) {
	// reduce interference from the other shrink path.
	DialQueueMaxIdle = 10 * time.Minute

	in := queue.NewChanQueue(context.Background(), queue.NewXORDistancePQ("test"))
	hang := make(chan struct{})

	wg := new(sync.WaitGroup)
	wg.Add(13)
	dialFn := func(ctx context.Context, p peer.ID) error {
		wg.Done()
		<-hang
		return nil
	}

	dq := newDialQueue(context.Background(), "test", in, dialFn)

	defer func() {
		recover()
		fmt.Println(dq.nWorkers)
	}()

	// acquire 3 consumers, everytime we acquire a consumer, we will grow the pool because no dial job is completed
	// and immediately returnable.
	for i := 0; i < 3; i++ {
		_ = dq.Consume()
	}

	// Enqueue 13 jobs, one per worker we'll grow to.
	for i := 0; i < 13; i++ {
		in.EnqChan <- peer.ID(i)
	}

	waitForWg(t, wg, 2*time.Second)

	// Release a few dialFn, but not all of them because downscaling happens when workers detect there are no
	// consumers to consume their values. So the other three will be these witnesses.
	for i := 0; i < 3; i++ {
		hang <- struct{}{}
	}

	// allow enough time for signalling and dispatching values to outstanding consumers.
	time.Sleep(1 * time.Second)

	// unblock the rest.
	for i := 0; i < 10; i++ {
		hang <- struct{}{}
	}

	wg = new(sync.WaitGroup)
	// we should now only have 6 workers, because all the shrink events will have been honoured.
	wg.Add(6)

	// enqueue more jobs.
	for i := 0; i < 6; i++ {
		in.EnqChan <- peer.ID(i)
	}

	// let's check we have 6 workers hanging.
	waitForWg(t, wg, 2*time.Second)
}

// Inactivity = workers are idle because the DHT query is progressing slow and is producing too few peers to dial.
func TestDialQueueShrinksWithWhenIdle(t *testing.T) {
	DialQueueMaxIdle = 1 * time.Second

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

	dq := newDialQueue(context.Background(), "test", in, dialFn)

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

	dq := newDialQueue(context.Background(), "test", in, dialFn)

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
