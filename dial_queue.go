package dht

import (
	"context"
	"math"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"
	queue "github.com/libp2p/go-libp2p-peerstore/queue"
)

var (
	DialQueueMinParallelism    = 6
	DialQueueMaxParallelism    = 20
	DialQueueMaxIdle           = 5 * time.Second
	DialQueueScalingMutePeriod = 1 * time.Second
)

type dialQueue struct {
	ctx    context.Context
	dialFn func(context.Context, peer.ID) error

	nWorkers      int
	scalingFactor float64

	in  *queue.ChanQueue
	out *queue.ChanQueue

	waitingCh chan waitingCh
	dieCh     chan struct{}
	growCh    chan struct{}
	shrinkCh  chan struct{}
}

type waitingCh struct {
	ch chan<- peer.ID
	ts time.Time
}

// newDialQueue returns an adaptive dial queue that spawns a dynamically sized set of goroutines to preemptively
// stage dials for later handoff to the DHT protocol for RPC. It identifies backpressure on both ends (dial consumers
// and dial producers), and takes compensating action by adjusting the worker pool.
//
// Why? Dialing is expensive. It's orders of magnitude slower than running an RPC on an already-established
// connection, as it requires establishing a TCP connection, multistream handshake, crypto handshake, mux handshake,
// and protocol negotiation.
//
// We start with DialQueueMinParallelism number of workers, and scale up and down based on demand and supply of
// dialled peers.
//
// The following events trigger scaling:
// - we scale up when we can't immediately return a successful dial to a new consumer.
// - we scale down when we've been idle for a while waiting for new dial attempts.
// - we scale down when we complete a dial and realise nobody was waiting for it.
//
// Dialler throttling (e.g. FD limit exceeded) is a concern, as we can easily spin up more workers to compensate, and
// end up adding fuel to the fire. Since we have no deterministic way to detect this for now, we hard-limit concurrency
// to DialQueueMaxParallelism.
func newDialQueue(ctx context.Context, target string, in *queue.ChanQueue, dialFn func(context.Context, peer.ID) error, nConsumers int) *dialQueue {
	sq := &dialQueue{
		ctx:           ctx,
		dialFn:        dialFn,
		nWorkers:      DialQueueMinParallelism,
		scalingFactor: 1.5,

		in:  in,
		out: queue.NewChanQueue(ctx, queue.NewXORDistancePQ(target)),

		growCh:    make(chan struct{}, nConsumers),
		shrinkCh:  make(chan struct{}, 1),
		waitingCh: make(chan waitingCh, nConsumers),
		dieCh:     make(chan struct{}, DialQueueMaxParallelism),
	}
	for i := 0; i < DialQueueMinParallelism; i++ {
		go sq.worker()
	}
	go sq.control()
	return sq
}

func (dq *dialQueue) control() {
	var (
		p              peer.ID
		dialled        = dq.out.DeqChan
		resp           waitingCh
		waiting        <-chan waitingCh
		lastScalingEvt = time.Now()
	)
	for {
		// First process any backlog of dial jobs and waiters -- making progress is the priority.
		// This block is copied below; couldn't find a more concise way of doing this.
		select {
		case <-dq.ctx.Done():
			return
		case p = <-dialled:
			dialled, waiting = nil, dq.waitingCh
			continue // onto the top.
		case resp = <-waiting:
			// got a channel that's waiting for a peer.
			log.Debugf("delivering dialled peer to DHT; took %dms.", time.Now().Sub(resp.ts)/time.Millisecond)
			resp.ch <- p
			close(resp.ch)
			dialled, waiting = dq.out.DeqChan, nil // stop consuming waiting jobs until we've cleared a peer.
			continue                               // onto the top.
		default:
			// there's nothing to process, so proceed onto the main select block.
		}

		select {
		case <-dq.ctx.Done():
			return
		case p = <-dialled:
			dialled, waiting = nil, dq.waitingCh
		case resp = <-waiting:
			// got a channel that's waiting for a peer.
			log.Debugf("delivering dialled peer to DHT; took %dms.", time.Now().Sub(resp.ts)/time.Millisecond)
			resp.ch <- p
			close(resp.ch)
			dialled, waiting = dq.out.DeqChan, nil // stop consuming waiting jobs until we've cleared a peer.
		case <-dq.growCh:
			if time.Now().Sub(lastScalingEvt) < DialQueueScalingMutePeriod {
				continue
			}
			dq.grow()
			lastScalingEvt = time.Now()
		case <-dq.shrinkCh:
			if time.Now().Sub(lastScalingEvt) < DialQueueScalingMutePeriod {
				continue
			}
			dq.shrink()
			lastScalingEvt = time.Now()
		}
	}
}

func (dq *dialQueue) Consume() <-chan peer.ID {
	ch := make(chan peer.ID, 1)

	select {
	case p := <-dq.out.DeqChan:
		// short circuit and return a dialled peer if it's immediately available.
		ch <- p
		close(ch)
		return ch
	case <-dq.ctx.Done():
		// return a closed channel with no value if we're done.
		close(ch)
		return ch
	default:
	}

	// we have no finished dials to return, trigger a scale up.
	select {
	case dq.growCh <- struct{}{}:
	default:
	}

	// park the channel until a dialled peer becomes available.
	select {
	case dq.waitingCh <- waitingCh{ch, time.Now()}:
	default:
		panic("detected more consuming goroutines than declared upfront")
	}
	return ch
}

func (dq *dialQueue) grow() {
	// no mutex needed as this is only called from the (single-threaded) control loop.
	defer func(prev int) {
		if prev == dq.nWorkers {
			return
		}
		log.Debugf("grew dial worker pool: %d => %d", prev, dq.nWorkers)
	}(dq.nWorkers)

	if dq.nWorkers == DialQueueMaxParallelism {
		return
	}
	target := int(math.Floor(float64(dq.nWorkers) * dq.scalingFactor))
	if target > DialQueueMaxParallelism {
		target = DialQueueMinParallelism
	}
	for ; dq.nWorkers < target; dq.nWorkers++ {
		go dq.worker()
	}
}

func (dq *dialQueue) shrink() {
	// no mutex needed as this is only called from the (single-threaded) control loop.
	defer func(prev int) {
		if prev == dq.nWorkers {
			return
		}
		log.Debugf("shrunk dial worker pool: %d => %d", prev, dq.nWorkers)
	}(dq.nWorkers)

	if dq.nWorkers == DialQueueMinParallelism {
		return
	}
	target := int(math.Floor(float64(dq.nWorkers) / dq.scalingFactor))
	if target < DialQueueMinParallelism {
		target = DialQueueMinParallelism
	}
	// send as many die signals as workers we have to prune.
	for ; dq.nWorkers > target; dq.nWorkers-- {
		select {
		case dq.dieCh <- struct{}{}:
		default:
			log.Debugf("too many die signals queued up.")
		}
	}
}

func (dq *dialQueue) worker() {
	// This idle timer tracks if the environment is slow. If we're waiting to long to acquire a peer to dial,
	// it means that the DHT query is progressing slow and we should shrink the worker pool.
	idleTimer := time.NewTimer(0)

	for {
		// trap exit signals first.
		select {
		case <-dq.ctx.Done():
			return
		case <-dq.dieCh:
			return
		default:
		}

		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(DialQueueMaxIdle)

		select {
		case <-dq.dieCh:
			return
		case <-dq.ctx.Done():
			return
		case <-idleTimer.C:
			// no new dial requests during our idle period; time to scale down.
		case p := <-dq.in.DeqChan:
			t := time.Now()
			if err := dq.dialFn(dq.ctx, p); err != nil {
				log.Debugf("discarding dialled peer because of error: %v", err)
				continue
			}
			log.Debugf("dialling %v took %dms (as observed by the dht subsystem).", p, time.Now().Sub(t)/time.Millisecond)
			waiting := len(dq.waitingCh)
			dq.out.EnqChan <- p
			if waiting > 0 {
				// we have somebody to deliver this value to, so no need to shrink.
				continue
			}
		}

		// scaling down; control only arrives here if the idle timer fires, or if there are no goroutines
		// waiting for the value we just produced.
		select {
		case dq.shrinkCh <- struct{}{}:
		default:
		}
	}
}
