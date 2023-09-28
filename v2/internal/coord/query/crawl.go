package query

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p-kad-dht/v2/tele"
	"go.opentelemetry.io/otel/trace"

	"github.com/plprobelab/go-kademlia/key"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/plprobelab/go-kademlia/kad"

	"github.com/benbjohnson/clock"
	"github.com/plprobelab/go-kademlia/kaderr"
)

// CrawlConfig specifies optional configuration for a Crawl
type CrawlConfig struct {
	MaxCPL      int         // the maximum CPL until we should crawl the peer
	Concurrency int         // the maximum number of concurrent peers that we may query
	Clock       clock.Clock // a clock that may replaced by a mock when testing
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *CrawlConfig) Validate() error {
	if cfg.Clock == nil {
		return &kaderr.ConfigurationError{
			Component: "CrawlConfig",
			Err:       fmt.Errorf("clock must not be nil"),
		}
	}
	return nil
}

// DefaultCrawlConfig returns the default configuration options for a Crawl.
// Options may be overridden before passing to NewCrawl
func DefaultCrawlConfig() *CrawlConfig {
	return &CrawlConfig{
		MaxCPL:      16,
		Concurrency: 1,
		Clock:       clock.New(), // use standard time
	}
}

type crawlJob[K kad.Key[K], N kad.NodeID[K]] struct {
	node   N
	target K
}

func (c *crawlJob[K, N]) mapKey() string {
	return c.node.String() + key.HexString(c.target)
}

type Crawl[K kad.Key[K], N kad.NodeID[K], M coordt.Message] struct {
	self N
	id   coordt.QueryID

	// cfg is a copy of the optional configuration supplied to the query
	cfg   CrawlConfig
	cplFn coordt.NodeIDForCplFunc[K, N]

	todo    []crawlJob[K, N]
	cpls    map[string]int
	waiting map[string]N
	success map[string]N
	failed  map[string]N
	errors  map[string]error
}

func NewCrawl[K kad.Key[K], N kad.NodeID[K], M coordt.Message](self N, id coordt.QueryID, cplFn coordt.NodeIDForCplFunc[K, N], seed []N, cfg *CrawlConfig) (*Crawl[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultCrawlConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	c := &Crawl[K, N, M]{
		self:    self,
		id:      id,
		cfg:     *cfg,
		cplFn:   cplFn,
		todo:    make([]crawlJob[K, N], 0, len(seed)*cfg.MaxCPL),
		cpls:    map[string]int{},
		waiting: map[string]N{},
		success: map[string]N{},
		failed:  map[string]N{},
		errors:  map[string]error{},
	}

	for _, node := range seed {
		// exclude self from closest nodes
		if key.Equal(node.Key(), self.Key()) {
			continue
		}

		for i := 0; i < c.cfg.MaxCPL; i++ {
			target, err := cplFn(node.Key(), i)
			if err != nil {
				return nil, fmt.Errorf("generate cpl: %w", err)
			}

			job := crawlJob[K, N]{
				node:   node,
				target: target.Key(),
			}

			c.cpls[job.mapKey()] = i
			c.todo = append(c.todo, job)
		}
	}

	return c, nil
}

func (c *Crawl[K, N, M]) Advance(ctx context.Context, ev CrawlEvent) (out CrawlState) {
	ctx, span := tele.StartSpan(ctx, "Crawl.Advance", trace.WithAttributes(tele.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(tele.AttrOutEvent(out))
		span.End()
	}()

	switch tev := ev.(type) {
	case *EventCrawlCancel:
		// TODO: ...
	case *EventCrawlNodeResponse[K, N]:
		job := crawlJob[K, N]{
			node:   tev.NodeID,
			target: tev.Target,
		}

		mapKey := job.mapKey()

		delete(c.waiting, mapKey)
		c.success[mapKey] = tev.NodeID

		for _, node := range tev.CloserNodes {
			for i := 0; i < c.cfg.MaxCPL; i++ {
				target, err := c.cplFn(node.Key(), i)
				if err != nil {
					// TODO: log
					continue
				}

				job := crawlJob[K, N]{
					node:   node,
					target: target.Key(),
				}

				mapKey := job.mapKey()

				if _, found := c.cpls[mapKey]; found {
					continue
				}

				c.cpls[mapKey] = i
				c.todo = append(c.todo, job)
			}
		}
	case *EventCrawlNodeFailure[K, N]:
		job := crawlJob[K, N]{
			node:   tev.NodeID,
			target: tev.Target,
		}

		mapKey := job.mapKey()

		delete(c.waiting, mapKey)
		c.failed[mapKey] = tev.NodeID
		c.errors[mapKey] = tev.Error
	case *EventCrawlPoll:
		// no event to process
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if len(c.waiting) >= c.cfg.MaxCPL*c.cfg.Concurrency {
		return &StateCrawlWaitingAtCapacity{
			QueryID: c.id,
		}
	}

	if len(c.todo) > 0 {

		// pop next crawl job from queue
		var job crawlJob[K, N]
		job, c.todo = c.todo[0], c.todo[1:]

		// mark the job as waiting
		c.waiting[job.mapKey()] = job.node

		return &StateCrawlFindCloser[K, N]{
			QueryID: c.id,
			Target:  job.target,
			NodeID:  job.node,
		}
	}

	if len(c.waiting) > 0 {
		return &StateCrawlWaitingWithCapacity{
			QueryID: c.id,
		}
	}

	return &StateCrawlIdle{}
}

type CrawlState interface {
	crawlState()
}

type StateCrawlIdle struct{}

type StateCrawlWaitingAtCapacity struct {
	QueryID coordt.QueryID
}
type StateCrawlWaitingWithCapacity struct {
	QueryID coordt.QueryID
}

type StateCrawlFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
}

// crawlState() ensures that only [Crawl] states can be assigned to a CrawlState.
func (*StateCrawlIdle) crawlState()                {}
func (*StateCrawlFindCloser[K, N]) crawlState()    {}
func (*StateCrawlWaitingAtCapacity) crawlState()   {}
func (*StateCrawlWaitingWithCapacity) crawlState() {}

type CrawlEvent interface {
	crawlEvent()
}

// EventCrawlPoll is an event that signals a [Crawl] that it can perform housekeeping work.
type EventCrawlPoll struct{}

type EventCrawlCancel struct{}

type EventCrawlNodeResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to
	Target      K   // the key that the node was asked for
	CloserNodes []N // the closer nodes sent by the node
}

type EventCrawlNodeFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to
	Target K     // the key that the node was asked for
	Error  error // the error that caused the failure, if any
}

// crawlEvent() ensures that only events accepted by [Crawl] can be assigned to a [CrawlEvent].
func (*EventCrawlPoll) crawlEvent()               {}
func (*EventCrawlCancel) crawlEvent()             {}
func (*EventCrawlNodeResponse[K, N]) crawlEvent() {}
func (*EventCrawlNodeFailure[K, N]) crawlEvent()  {}
