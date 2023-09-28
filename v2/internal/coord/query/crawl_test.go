package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/benbjohnson/clock"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/coordt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/internal/tiny"
	"github.com/stretchr/testify/require"
)

var _ coordt.StateMachine[CrawlEvent, CrawlState] = (*Crawl[tiny.Key, tiny.Node, tiny.Message])(nil)

func TestCrawl_Advance(t *testing.T) {
	ctx := context.Background()

	self := tiny.NewNode(0)
	a := tiny.NewNode(0b10000100)
	b := tiny.NewNode(0b11000000)
	c := tiny.NewNode(0b10100000)
	seed := []tiny.Node{self, a, b}

	clk := clock.NewMock()

	cfg := DefaultCrawlConfig()
	cfg.Clock = clk
	cfg.MaxCPL = 4
	cfg.Concurrency = 2

	queryID := coordt.QueryID("test")

	qry, err := NewCrawl[tiny.Key, tiny.Node, tiny.Message](self, queryID, tiny.NodeWithCpl, seed, cfg)
	require.NoError(t, err)

	assert.Len(t, qry.todo, 2*cfg.MaxCPL)
	assert.Len(t, qry.cpls, 2*cfg.MaxCPL)
	assert.Len(t, qry.waiting, 0)
	assert.Len(t, qry.success, 0)
	assert.Len(t, qry.failed, 0)

	reqs := make([]*StateCrawlFindCloser[tiny.Key, tiny.Node], 2*cfg.MaxCPL)
	for i := 0; i < 2*cfg.MaxCPL; i++ {
		state := qry.Advance(ctx, &EventCrawlPoll{})
		tstate, ok := state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
		require.True(t, ok, "type is %T", state)
		reqs[i] = tstate
	}

	assert.Len(t, qry.todo, 0)
	assert.Len(t, qry.cpls, 2*cfg.MaxCPL)
	assert.Len(t, qry.waiting, 2*cfg.MaxCPL)
	assert.Len(t, qry.success, 0)
	assert.Len(t, qry.failed, 0)

	state := qry.Advance(ctx, &EventCrawlPoll{})
	require.IsType(t, &StateCrawlWaitingAtCapacity{}, state)

	state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      reqs[0].NodeID,
		Target:      reqs[0].Target,
		CloserNodes: []tiny.Node{},
	})
	require.IsType(t, &StateCrawlWaitingWithCapacity{}, state)

	assert.Len(t, qry.todo, 0)
	assert.Len(t, qry.cpls, 2*cfg.MaxCPL)
	assert.Len(t, qry.waiting, 2*cfg.MaxCPL-1)
	assert.Len(t, qry.success, 1)
	assert.Len(t, qry.failed, 0)

	state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      reqs[1].NodeID,
		Target:      reqs[1].Target,
		CloserNodes: []tiny.Node{c},
	})

	tstate, ok := state.(*StateCrawlFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "type is %T", state)
	assert.Equal(t, tstate.NodeID, c)

	assert.Len(t, qry.todo, 3)
	assert.Len(t, qry.cpls, 3*cfg.MaxCPL)
	assert.Len(t, qry.waiting, 2*cfg.MaxCPL-1)
	assert.Len(t, qry.success, 2)
	assert.Len(t, qry.failed, 0)

	for i := 2; i < len(reqs); i++ {
		state = qry.Advance(ctx, &EventCrawlNodeResponse[tiny.Key, tiny.Node]{
			NodeID:      reqs[i].NodeID,
			Target:      reqs[i].Target,
			CloserNodes: []tiny.Node{},
		})
	}

	require.IsType(t, &StateCrawlIdle{}, state)
}
