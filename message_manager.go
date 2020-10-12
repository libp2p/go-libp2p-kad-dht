package dht

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"

	"github.com/libp2p/go-msgio"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

type messageManager struct {
	host      host.Host // the network services we need
	smlk      sync.Mutex
	strmap    map[peer.ID]*messageSender
	protocols []protocol.ID
}

func (m *messageManager) streamDisconnect(ctx context.Context, p peer.ID) {
	m.smlk.Lock()
	defer m.smlk.Unlock()
	ms, ok := m.strmap[p]
	if !ok {
		return
	}
	delete(m.strmap, p)

	// Do this asynchronously as ms.lk can block for a while.
	go func() {
		if err := ms.lk.Lock(ctx); err != nil {
			return
		}
		defer ms.lk.Unlock()
		ms.invalidate()
	}()
}

// SendRequest sends out a request, but also makes sure to
// measure the RTT for latency measurements.
func (m *messageManager) SendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))

	ms, err := m.messageSenderForPeer(ctx, p)
	if err != nil {
		stats.Record(ctx,
			metrics.SentRequests.M(1),
			metrics.SentRequestErrors.M(1),
		)
		logger.Debugw("request failed to open message sender", "error", err, "to", p)
		return nil, err
	}

	start := time.Now()

	rpmes, err := ms.SendRequest(ctx, pmes)
	if err != nil {
		stats.Record(ctx,
			metrics.SentRequests.M(1),
			metrics.SentRequestErrors.M(1),
		)
		logger.Debugw("request failed", "error", err, "to", p)
		return nil, err
	}

	stats.Record(ctx,
		metrics.SentRequests.M(1),
		metrics.SentBytes.M(int64(pmes.Size())),
		metrics.OutboundRequestLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
	)
	m.host.Peerstore().RecordLatency(p, time.Since(start))
	return rpmes, nil
}

// SendMessage sends out a message
func (m *messageManager) SendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))

	ms, err := m.messageSenderForPeer(ctx, p)
	if err != nil {
		stats.Record(ctx,
			metrics.SentMessages.M(1),
			metrics.SentMessageErrors.M(1),
		)
		logger.Debugw("message failed to open message sender", "error", err, "to", p)
		return err
	}

	if err := ms.SendMessage(ctx, pmes); err != nil {
		stats.Record(ctx,
			metrics.SentMessages.M(1),
			metrics.SentMessageErrors.M(1),
		)
		logger.Debugw("message failed", "error", err, "to", p)
		return err
	}

	stats.Record(ctx,
		metrics.SentMessages.M(1),
		metrics.SentBytes.M(int64(pmes.Size())),
	)
	return nil
}

func (m *messageManager) messageSenderForPeer(ctx context.Context, p peer.ID) (*messageSender, error) {
	m.smlk.Lock()
	ms, ok := m.strmap[p]
	if ok {
		m.smlk.Unlock()
		return ms, nil
	}
	ms = &messageSender{p: p, m: m, lk: newCtxMutex()}
	m.strmap[p] = ms
	m.smlk.Unlock()

	if err := ms.prepOrInvalidate(ctx); err != nil {
		m.smlk.Lock()
		defer m.smlk.Unlock()

		if msCur, ok := m.strmap[p]; ok {
			// Changed. Use the new one, old one is invalid and
			// not in the map so we can just throw it away.
			if ms != msCur {
				return msCur, nil
			}
			// Not changed, remove the now invalid stream from the
			// map.
			delete(m.strmap, p)
		}
		// Invalid but not in map. Must have been removed by a disconnect.
		return nil, err
	}
	// All ready to go.
	return ms, nil
}

type messageSender struct {
	s  network.Stream
	r  msgio.ReadCloser
	lk ctxMutex
	p  peer.ID
	m  *messageManager

	invalid   bool
	singleMes int
}

// invalidate is called before this messageSender is removed from the strmap.
// It prevents the messageSender from being reused/reinitialized and then
// forgotten (leaving the stream open).
func (ms *messageSender) invalidate() {
	ms.invalid = true
	if ms.s != nil {
		_ = ms.s.Reset()
		ms.s = nil
	}
}

func (ms *messageSender) prepOrInvalidate(ctx context.Context) error {
	if err := ms.lk.Lock(ctx); err != nil {
		return err
	}
	defer ms.lk.Unlock()

	if err := ms.prep(ctx); err != nil {
		ms.invalidate()
		return err
	}
	return nil
}

func (ms *messageSender) prep(ctx context.Context) error {
	if ms.invalid {
		return fmt.Errorf("message sender has been invalidated")
	}
	if ms.s != nil {
		return nil
	}

	// We only want to speak to peers using our primary protocols. We do not want to query any peer that only speaks
	// one of the secondary "server" protocols that we happen to support (e.g. older nodes that we can respond to for
	// backwards compatibility reasons).
	nstr, err := ms.m.host.NewStream(ctx, ms.p, ms.m.protocols...)
	if err != nil {
		return err
	}

	ms.r = msgio.NewVarintReaderSize(nstr, network.MessageSizeMax)
	ms.s = nstr

	return nil
}

// streamReuseTries is the number of times we will try to reuse a stream to a
// given peer before giving up and reverting to the old one-message-per-stream
// behaviour.
const streamReuseTries = 3

func (ms *messageSender) SendMessage(ctx context.Context, pmes *pb.Message) error {
	if err := ms.lk.Lock(ctx); err != nil {
		return err
	}
	defer ms.lk.Unlock()

	retry := false
	for {
		if err := ms.prep(ctx); err != nil {
			return err
		}

		if err := ms.writeMsg(pmes); err != nil {
			_ = ms.s.Reset()
			ms.s = nil

			if retry {
				logger.Debugw("error writing message", "error", err)
				return err
			}
			logger.Debugw("error writing message", "error", err, "retrying", true)
			retry = true
			continue
		}

		var err error
		if ms.singleMes > streamReuseTries {
			err = ms.s.Close()
			ms.s = nil
		} else if retry {
			ms.singleMes++
		}

		return err
	}
}

func (ms *messageSender) SendRequest(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
	if err := ms.lk.Lock(ctx); err != nil {
		return nil, err
	}
	defer ms.lk.Unlock()

	retry := false
	for {
		if err := ms.prep(ctx); err != nil {
			return nil, err
		}

		if err := ms.writeMsg(pmes); err != nil {
			_ = ms.s.Reset()
			ms.s = nil

			if retry {
				logger.Debugw("error writing message", "error", err)
				return nil, err
			}
			logger.Debugw("error writing message", "error", err, "retrying", true)
			retry = true
			continue
		}

		mes := new(pb.Message)
		if err := ms.ctxReadMsg(ctx, mes); err != nil {
			_ = ms.s.Reset()
			ms.s = nil

			if retry {
				logger.Debugw("error reading message", "error", err)
				return nil, err
			}
			logger.Debugw("error reading message", "error", err, "retrying", true)
			retry = true
			continue
		}

		var err error
		if ms.singleMes > streamReuseTries {
			err = ms.s.Close()
			ms.s = nil
		} else if retry {
			ms.singleMes++
		}

		return mes, err
	}
}

func (ms *messageSender) writeMsg(pmes *pb.Message) error {
	return writeMsg(ms.s, pmes)
}

func (ms *messageSender) ctxReadMsg(ctx context.Context, mes *pb.Message) error {
	errc := make(chan error, 1)
	go func(r msgio.ReadCloser) {
		defer close(errc)
		bytes, err := r.ReadMsg()
		defer r.ReleaseMsg(bytes)
		if err != nil {
			errc <- err
			return
		}
		errc <- mes.Unmarshal(bytes)
	}(ms.r)

	t := time.NewTimer(dhtReadMessageTimeout)
	defer t.Stop()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrReadTimeout
	}
}
