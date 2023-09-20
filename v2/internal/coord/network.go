package coord

import (
	"context"
	"fmt"
	"sync"

	"github.com/plprobelab/go-kademlia/key"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/internal/coord/query"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

type NetworkBehaviour struct {
	// rtr is the message router used to send messages
	rtr Router[kadt.Key, kadt.PeerID, *pb.Message]

	nodeHandlersMu sync.Mutex
	nodeHandlers   map[kadt.PeerID]*NodeHandler // TODO: garbage collect node handlers

	pendingMu sync.Mutex
	pending   []BehaviourEvent
	ready     chan struct{}

	logger *slog.Logger
	tracer trace.Tracer
}

func NewNetworkBehaviour(rtr Router[kadt.Key, kadt.PeerID, *pb.Message], logger *slog.Logger, tracer trace.Tracer) *NetworkBehaviour {
	b := &NetworkBehaviour{
		rtr:          rtr,
		nodeHandlers: make(map[kadt.PeerID]*NodeHandler),
		ready:        make(chan struct{}, 1),
		logger:       logger.With("behaviour", "network"),
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
		p := kadt.PeerID(ev.To)
		nh, ok := b.nodeHandlers[p]
		if !ok {
			nh = NewNodeHandler(p, b.rtr, b.logger, b.tracer)
			b.nodeHandlers[p] = nh
		}
		b.nodeHandlersMu.Unlock()
		nh.Notify(ctx, ev)
	case *EventOutboundSendMessage:
		b.nodeHandlersMu.Lock()
		p := kadt.PeerID(ev.To)
		nh, ok := b.nodeHandlers[p]
		if !ok {
			nh = NewNodeHandler(p, b.rtr, b.logger, b.tracer)
			b.nodeHandlers[p] = nh
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

func (b *NetworkBehaviour) getNodeHandler(ctx context.Context, id kadt.PeerID) (*NodeHandler, error) {
	b.nodeHandlersMu.Lock()
	nh, ok := b.nodeHandlers[id]
	if !ok {
		nh = NewNodeHandler(id, b.rtr, b.logger, b.tracer)
		b.nodeHandlers[id] = nh
	}
	b.nodeHandlersMu.Unlock()
	return nh, nil
}

type NodeHandler struct {
	self   kadt.PeerID
	rtr    Router[kadt.Key, kadt.PeerID, *pb.Message]
	queue  *WorkQueue[NodeHandlerRequest]
	logger *slog.Logger
	tracer trace.Tracer
}

func NewNodeHandler(self kadt.PeerID, rtr Router[kadt.Key, kadt.PeerID, *pb.Message], logger *slog.Logger, tracer trace.Tracer) *NodeHandler {
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
	case *EventOutboundSendMessage:
		if cmd.Notify == nil {
			break
		}
		resp, err := h.rtr.SendMessage(ctx, h.self, cmd.Message)
		if err != nil {
			cmd.Notify.Notify(ctx, &EventSendMessageFailure{
				QueryID: cmd.QueryID,
				To:      h.self,
				Err:     fmt.Errorf("NodeHandler: %w", err),
			})
			return false
		}

		cmd.Notify.Notify(ctx, &EventSendMessageSuccess{
			QueryID:     cmd.QueryID,
			To:          h.self,
			Response:    resp,
			CloserNodes: resp.CloserNodes(),
		})
	default:
		panic(fmt.Sprintf("unexpected command type: %T", cmd))
	}

	return false
}

func (h *NodeHandler) ID() kadt.PeerID {
	return h.self
}

// GetClosestNodes requests the n closest nodes to the key from the node's local routing table.
// The node may return fewer nodes than requested.
func (h *NodeHandler) GetClosestNodes(ctx context.Context, k kadt.Key, n int) ([]Node, error) {
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
func (h *NodeHandler) GetValue(ctx context.Context, key kadt.Key) (Value, error) {
	panic("not implemented")
}

// PutValue requests that the node stores a value to be associated with the supplied key.
// If the node cannot or chooses not to store the value for the key it returns ErrValueNotAccepted.
func (h *NodeHandler) PutValue(ctx context.Context, r Value, q int) error {
	panic("not implemented")
}
