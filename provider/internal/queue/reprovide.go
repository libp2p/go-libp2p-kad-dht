package queue

import (
	"sync"

	"github.com/gammazero/deque"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

type ReprovideQueue struct {
	lk sync.Mutex

	queue    deque.Deque[bitstr.Key]          // used to preserve the queue order
	prefixes *trie.Trie[bitstr.Key, struct{}] // used to track prefixes in the queue
}

// New creates a new ProvideQueue instance.
func NewReprovideQueue() *ReprovideQueue {
	return nil
}
