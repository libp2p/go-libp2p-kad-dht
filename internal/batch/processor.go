package batch

import (
	"fmt"
	"sync"

	mh "github.com/multiformats/go-multihash"
)

type BatchHandler func(keys []mh.Multihash, special bool)

const (
	normalQueueID uint8 = iota
	specialQueueID
)

type enqueueOp struct {
	keys []mh.Multihash
	id   uint8
}

type drainOp struct {
	id uint8
	ch chan []mh.Multihash
}

type Processor struct {
	close     chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	enqueue   chan enqueueOp
	drain     chan drainOp

	signals [2]chan struct{}
	bufs    [2][]mh.Multihash

	f BatchHandler
}

func NewProcessor(f BatchHandler) *Processor {
	q := &Processor{
		close:   make(chan struct{}),
		wg:      sync.WaitGroup{},
		enqueue: make(chan enqueueOp),
		drain:   make(chan drainOp),

		signals: [2]chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1)},
		bufs:    [2][]mh.Multihash{make([]mh.Multihash, 0), make([]mh.Multihash, 0)},
		f:       f,
	}
	q.wg.Add(2)
	go q.in()
	go q.out()
	return q
}

func (q *Processor) in() {
	defer q.wg.Done()

	for {
		select {
		case <-q.close:
			return
		case op := <-q.enqueue:
			q.bufs[op.id] = append(q.bufs[op.id], op.keys...)
			// Signal there are keys in the queue.
			select {
			case q.signals[op.id] <- struct{}{}:
			default:
			}
		case op := <-q.drain:
			op.ch <- q.bufs[op.id]
			q.bufs[op.id] = make([]mh.Multihash, 0)
		}
	}
}

func (q *Processor) out() {
	defer q.wg.Done()

	ch := make(chan []mh.Multihash)
	handle := func(id uint8) {
		select {
		case <-q.close:
			return
		case q.drain <- drainOp{id: id, ch: ch}:
		}

		var keys []mh.Multihash
		select {
		case <-q.close:
			return
		case keys = <-ch:
		}

		// Run callback
		q.f(keys, id == specialQueueID)
	}

	for {
		select {
		case <-q.close:
			return
		case <-q.signals[normalQueueID]:
			handle(normalQueueID)
		case <-q.signals[specialQueueID]:
			handle(specialQueueID)
		}
	}
}

func (q *Processor) Close() error {
	q.closeOnce.Do(func() {
		close(q.enqueue)
		close(q.close)
	})
	return nil
}

func (q *Processor) Enqueue(special bool, keys ...mh.Multihash) (err error) {
	id := normalQueueID
	if special {
		id = specialQueueID
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to enqueue item: %s", r)
		}
	}()
	q.enqueue <- enqueueOp{keys: keys, id: id}
	return
}
