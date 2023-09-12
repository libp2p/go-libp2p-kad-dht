package coord

import (
	"context"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
)

type NetworkBehaviour struct {
	// rtr is the message router used to send messages
	rtr Router

	nodeHandlersMu sync.Mutex
	nodeHandlers   map[peer.ID]*NodeHandler // TODO: garbage collect node handlers

	pendingMu sync.Mutex
	pending   []BehaviourEvent
	ready     chan struct{}

	logger *slog.Logger
	tracer trace.Tracer
}

func NewNetworkBehaviour(rtr Router, logger *slog.Logger, tracer trace.Tracer) *NetworkBehaviour {
	b := &NetworkBehaviour{
		rtr:          rtr,
		nodeHandlers: make(map[peer.ID]*NodeHandler),
		ready:        make(chan struct{}, 1),
		logger:       logger,
		tracer:       tracer,
	}

	return b
}

func (b *NetworkBehaviour) Notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := b.tracer.Start(ctx, "NetworkBehaviour.Notify")
	defer span.End()

	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	switch ev := ev.(type) {
	case *EventOutboundGetCloserNodes:
		b.nodeHandlersMu.Lock()
		nh, ok := b.nodeHandlers[ev.To.ID]
		if !ok {
			nh = NewNodeHandler(ev.To, b.rtr, b.logger, b.tracer)
			b.nodeHandlers[ev.To.ID] = nh
		}
		b.nodeHandlersMu.Unlock()
		nh.Notify(ctx, ev)
	default:
		panic(fmt.Sprintf("unexpected dht event: %T", ev))
	}

	if len(b.pending) > 0 {
		select {
		case b.ready <- struct{}{}:
		default:
		}
	}
}

func (b *NetworkBehaviour) Ready() <-chan struct{} {
	return b.ready
}

func (b *NetworkBehaviour) Perform(ctx context.Context) (BehaviourEvent, bool) {
	_, span := b.tracer.Start(ctx, "NetworkBehaviour.Perform")
	defer span.End()
	// No inbound work can be done until Perform is complete
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	// drain queued events.
	if len(b.pending) > 0 {
		var ev BehaviourEvent
		ev, b.pending = b.pending[0], b.pending[1:]

		if len(b.pending) > 0 {
			select {
			case b.ready <- struct{}{}:
			default:
			}
		}
		return ev, true
	}

	return nil, false
}

func (b *NetworkBehaviour) getNodeHandler(ctx context.Context, id peer.ID) (*NodeHandler, error) {
	b.nodeHandlersMu.Lock()
	nh, ok := b.nodeHandlers[id]
	if !ok || len(nh.Addresses()) == 0 {
		info, err := b.rtr.GetNodeInfo(ctx, id)
		if err != nil {
			return nil, err
		}
		nh = NewNodeHandler(info, b.rtr, b.logger, b.tracer)
		b.nodeHandlers[id] = nh
	}
	b.nodeHandlersMu.Unlock()
	return nh, nil
}

type NodeHandler struct {
	self   peer.AddrInfo
	rtr    Router
	queue  *WorkQueue[NodeHandlerRequest]
	logger *slog.Logger
	tracer trace.Tracer
}

func NewNodeHandler(self peer.AddrInfo, rtr Router, logger *slog.Logger, tracer trace.Tracer) *NodeHandler {
	h := &NodeHandler{
		self:   self,
		rtr:    rtr,
		logger: logger,
		tracer: tracer,
	}

	h.queue = NewWorkQueue(h.send)

	return h
}

func (h *NodeHandler) Notify(ctx context.Context, ev NodeHandlerRequest) {
	ctx, span := h.tracer.Start(ctx, "NodeHandler.Notify")
	defer span.End()
	h.queue.Enqueue(ctx, ev)
}

func (h *NodeHandler) send(ctx context.Context, ev NodeHandlerRequest) bool {
	switch cmd := ev.(type) {
	case *EventOutboundGetCloserNodes:
		if cmd.Notify == nil {
			break
		}
		nodes, err := h.rtr.GetClosestNodes(ctx, h.self, cmd.Target)
		if err != nil {
			cmd.Notify.Notify(ctx, &EventGetCloserNodesFailure{
				QueryID: cmd.QueryID,
				To:      h.self,
				Target:  cmd.Target,
				Err:     fmt.Errorf("NodeHandler: %w", err),
			})
			return false
		}

		cmd.Notify.Notify(ctx, &EventGetCloserNodesSuccess{
			QueryID:     cmd.QueryID,
			To:          h.self,
			Target:      cmd.Target,
			CloserNodes: nodes,
		})
	default:
		panic(fmt.Sprintf("unexpected command type: %T", cmd))
	}

	return false
}

func (h *NodeHandler) ID() peer.ID {
	return h.self.ID
}

func (h *NodeHandler) Addresses() []ma.Multiaddr {
	return h.self.Addrs
}

// GetClosestNodes requests the n closest nodes to the key from the node's local routing table.
// The node may return fewer nodes than requested.
func (h *NodeHandler) GetClosestNodes(ctx context.Context, k KadKey, n int) ([]Node, error) {
	ctx, span := h.tracer.Start(ctx, "NodeHandler.GetClosestNodes")
	defer span.End()
	w := NewWaiter[BehaviourEvent]()

	ev := &EventOutboundGetCloserNodes{
		QueryID: query.QueryID(key.HexString(k)),
		To:      h.self,
		Target:  k,
		Notify:  w,
	}

	h.queue.Enqueue(ctx, ev)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case we := <-w.Chan():

		switch res := we.Event.(type) {
		case *EventGetCloserNodesSuccess:
			nodes := make([]Node, 0, len(res.CloserNodes))
			for _, info := range res.CloserNodes {
				// TODO use a global registry of node handlers
				nodes = append(nodes, NewNodeHandler(info, h.rtr, h.logger, h.tracer))
				n--
				if n == 0 {
					break
				}
			}
			return nodes, nil

		case *EventGetCloserNodesFailure:
			return nil, res.Err
		default:
			panic(fmt.Sprintf("unexpected node handler event: %T", ev))
		}
	}
}

// GetValue requests that the node return any value associated with the supplied key.
// If the node does not have a value for the key it returns ErrValueNotFound.
func (h *NodeHandler) GetValue(ctx context.Context, key KadKey) (Value, error) {
	panic("not implemented")
}

// PutValue requests that the node stores a value to be associated with the supplied key.
// If the node cannot or chooses not to store the value for the key it returns ErrValueNotAccepted.
func (h *NodeHandler) PutValue(ctx context.Context, r Value, q int) error {
	panic("not implemented")
}

func CloserNodeIDs(nodes []peer.AddrInfo) []kad.NodeID[KadKey] {
	ids := make([]kad.NodeID[KadKey], len(nodes))
	for i := range nodes {
		ids[i] = kadt.PeerID(nodes[i].ID)
	}

	return ids
}

type fakeMessage struct {
	key   KadKey
	infos []kad.NodeInfo[KadKey, ma.Multiaddr]
}

func (r fakeMessage) Target() KadKey {
	return r.key
}

func (r fakeMessage) CloserNodes() []kad.NodeInfo[KadKey, ma.Multiaddr] {
	return r.infos
}

func (r fakeMessage) EmptyResponse() kad.Response[KadKey, ma.Multiaddr] {
	return &fakeMessage{}
}
