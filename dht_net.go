package dht

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	util "github.com/ipfs/go-ipfs-util"
	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p-kbucket/keyspace"
	multiaddr "github.com/multiformats/go-multiaddr"

	msmux "github.com/multiformats/go-multistream"

	ggio "github.com/gogo/protobuf/io"

	msgio "github.com/libp2p/go-msgio"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var dhtReadMessageTimeout = time.Minute
var dhtStreamIdleTimeout = 10 * time.Minute
var ErrReadTimeout = fmt.Errorf("timed out reading response")

// The Protobuf writer performs multiple small writes when writing a message.
// We need to buffer those writes, to make sure that we're not sending a new
// packet for every single write.
type bufferedDelimitedWriter struct {
	*bufio.Writer
	ggio.WriteCloser
}

var writerPool = sync.Pool{
	New: func() interface{} {
		w := bufio.NewWriter(nil)
		return &bufferedDelimitedWriter{
			Writer:      w,
			WriteCloser: ggio.NewDelimitedWriter(w),
		}
	},
}

func writeMsg(w io.Writer, mes *pb.Message) error {
	bw := writerPool.Get().(*bufferedDelimitedWriter)
	bw.Reset(w)
	err := bw.WriteMsg(mes)
	if err == nil {
		err = bw.Flush()
	}
	bw.Reset(nil)
	writerPool.Put(bw)
	return err
}

func (w *bufferedDelimitedWriter) Flush() error {
	return w.Writer.Flush()
}

// handleNewStream implements the network.StreamHandler
func (dht *IpfsDHT) handleNewStream(s network.Stream) {
	defer s.Reset()
	if dht.handleNewMessage(s) {
		// Gracefully close the stream for writes.
		s.Close()
	}
}

// Returns true on orderly completion of writes (so we can Close the stream).
func (dht *IpfsDHT) handleNewMessage(s network.Stream) bool {
	ctx := dht.ctx
	r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	mPeer := s.Conn().RemotePeer()
	addr := s.Conn().RemoteMultiaddr()

	timer := time.AfterFunc(dhtStreamIdleTimeout, func() { s.Reset() })
	defer timer.Stop()

	for {
		if dht.getMode() != ModeServer {
			logger.Errorf("ignoring incoming dht message while not in server mode")
			return false
		}

		var req pb.Message
		msgbytes, err := r.ReadMsg()
		if err != nil {
			defer r.ReleaseMsg(msgbytes)
			if err == io.EOF {
				return true
			}
			// This string test is necessary because there isn't a single stream reset error
			// instance	in use.
			if err.Error() != "stream reset" {
				logger.Debugf("error reading message: %#v", err)
			}
			stats.RecordWithTags(
				ctx,
				[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
				metrics.ReceivedMessageErrors.M(1),
			)
			return false
		}
		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			logger.Debugf("error unmarshalling message: %#v", err)
			stats.RecordWithTags(
				ctx,
				[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
				metrics.ReceivedMessageErrors.M(1),
			)
			return false
		}

		timer.Reset(dhtStreamIdleTimeout)

		startTime := time.Now()
		ctx, _ = tag.New(
			ctx,
			tag.Upsert(metrics.KeyMessageType, req.GetType().String()),
		)

		stats.Record(
			ctx,
			metrics.ReceivedMessages.M(1),
			metrics.ReceivedBytes.M(int64(req.Size())),
		)

		c, _ := cid.Cast(req.Key)
		pid, _ := peer.IDFromBytes(req.Key)

		// note: MessageType implements Stringer.
		logger.Debugf("[inbound rpc] handling incoming message; from_peer=%s, type=%s, cid_key=%s, peer_key=%s, raw_key=%x, closer=%d, providers=%d, remote_addr=%s",
			mPeer, req.GetType(), c, pid, req.Key, len(req.GetCloserPeers()), len(req.ProviderPeers), addr)

		handler := dht.handlerForMsgType(req.GetType())
		if handler == nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			logger.Warningf("can't handle received message of type %v", req.GetType())
			return false
		}

		resp, err := handler(ctx, mPeer, &req, s.Conn())
		if err != nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			logger.Debugf("error handling message: %v", err)
			return false
		}

		dht.updateFromMessage(ctx, s.Conn(), &req)

		if resp == nil {
			continue
		}

		logger.Debugf("[inbound rpc] writing response message; to_peer=%s, type=%s, cid_key=%s, peer_key=%s, raw_key=%x, closer=%d, providers=%d, remote_addr=%s",
			mPeer, resp.GetType(), c, pid, req.Key, len(resp.GetCloserPeers()), len(resp.ProviderPeers), addr)

		// send out response msg
		err = writeMsg(s, resp)
		if err != nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			logger.Debugf("error writing response: %v", err)
			return false
		}

		elapsedTime := time.Since(startTime)
		latencyMillis := float64(elapsedTime) / float64(time.Millisecond)

		// note: MessageType implements Stringer.
		logger.Debugf("[inbound rpc] wrote response message; to_peer=%s, type=%s, cid_key=%s, peer_key=%s, raw_key=%x, closer=%d, providers=%d, elapsed_ms=%f, remote_addr=%s",
			mPeer, resp.GetType(), c, pid, req.Key, len(resp.GetCloserPeers()), len(resp.ProviderPeers), latencyMillis, addr)

		stats.Record(ctx, metrics.InboundRequestLatency.M(latencyMillis))
	}
}

// sendRequest sends out a request, but also makes sure to
// measure the RTT for latency measurements.
func (dht *IpfsDHT) sendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))

	ms, err := dht.messageSenderForPeer(ctx, p)
	if err != nil {
		if err == msmux.ErrNotSupported {
			dht.RoutingTable().Remove(p)
		}
		stats.Record(ctx, metrics.SentRequestErrors.M(1))
		return nil, err
	}

	start := time.Now()

	rpmes, err := ms.SendRequest(ctx, pmes)
	if err != nil {
		if err == msmux.ErrNotSupported {
			dht.RoutingTable().Remove(p)
		}
		stats.Record(ctx, metrics.SentRequestErrors.M(1))
		return nil, err
	}

	// update the peer (on valid msgs only)
	if ms.s == nil {
		if err = ms.prepOrInvalidate(ctx); err != nil {
			return nil, err
		}
	}
	dht.updateFromMessage(ctx, ms.s.Conn(), rpmes)

	stats.Record(
		ctx,
		metrics.SentRequests.M(1),
		metrics.SentBytes.M(int64(pmes.Size())),
		metrics.OutboundRequestLatency.M(
			float64(time.Since(start))/float64(time.Millisecond),
		),
	)
	dht.peerstore.RecordLatency(p, time.Since(start))
	logger.Event(ctx, "dhtReceivedMessage", dht.self, p, rpmes)
	return rpmes, nil
}

// sendMessage sends out a message
func (dht *IpfsDHT) sendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))

	ms, err := dht.messageSenderForPeer(ctx, p)
	if err != nil {
		if err == msmux.ErrNotSupported {
			dht.RoutingTable().Remove(p)
		}
		stats.Record(ctx, metrics.SentMessageErrors.M(1))
		return err
	}

	if err := ms.SendMessage(ctx, pmes); err != nil {
		if err == msmux.ErrNotSupported {
			dht.RoutingTable().Remove(p)
		}
		stats.Record(ctx, metrics.SentMessageErrors.M(1))
		return err
	}

	stats.Record(
		ctx,
		metrics.SentMessages.M(1),
		metrics.SentBytes.M(int64(pmes.Size())),
	)
	logger.Event(ctx, "dhtSentMessage", dht.self, p, pmes)
	return nil
}

func (dht *IpfsDHT) updateFromMessage(ctx context.Context, c network.Conn, mes *pb.Message) error {
	// Make sure that this node is actually a DHT server, not just a client.
	protos, err := dht.peerstore.SupportsProtocols(c.RemotePeer(), dht.protocolStrs()...)
	if err == nil && len(protos) > 0 {
		dht.UpdateConn(ctx, c)
	}
	return nil
}

func (dht *IpfsDHT) messageSenderForPeer(ctx context.Context, p peer.ID) (*messageSender, error) {
	dht.smlk.Lock()
	ms, ok := dht.strmap[p]
	if ok {
		dht.smlk.Unlock()
		return ms, nil
	}
	ms = &messageSender{p: p, dht: dht}
	dht.strmap[p] = ms
	dht.smlk.Unlock()

	if err := ms.prepOrInvalidate(ctx); err != nil {
		dht.smlk.Lock()
		defer dht.smlk.Unlock()

		if msCur, ok := dht.strmap[p]; ok {
			// Changed. Use the new one, old one is invalid and
			// not in the map so we can just throw it away.
			if ms != msCur {
				return msCur, nil
			}
			// Not changed, remove the now invalid stream from the
			// map.
			delete(dht.strmap, p)
		}
		// Invalid but not in map. Must have been removed by a disconnect.
		return nil, err
	}
	// All ready to go.
	return ms, nil
}

type messageSender struct {
	s   network.Stream
	r   msgio.ReadCloser
	lk  sync.Mutex
	p   peer.ID
	dht *IpfsDHT

	invalid   bool
	singleMes int
}

// invalidate is called before this messageSender is removed from the strmap.
// It prevents the messageSender from being reused/reinitialized and then
// forgotten (leaving the stream open).
func (ms *messageSender) invalidate() {
	ms.invalid = true
	if ms.s != nil {
		ms.s.Reset()
		ms.s = nil
	}
}

func (ms *messageSender) prepOrInvalidate(ctx context.Context) error {
	ms.lk.Lock()
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

	nstr, err := ms.dht.host.NewStream(ctx, ms.p, ms.dht.protocols...)
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
	ms.lk.Lock()
	defer ms.lk.Unlock()
	retry := false

	c, _ := cid.Cast(pmes.Key)
	pid, _ := peer.IDFromBytes(pmes.Key)

	distb := xor(keyspace.XORKeySpace.Key(pmes.Key).Bytes, keyspace.XORKeySpace.Key([]byte(ms.p)).Bytes)
	distance := keyspace.ZeroPrefixLen(distb)
	var addr multiaddr.Multiaddr
	if ms.s != nil {
		addr = ms.s.Conn().RemoteMultiaddr()
	}

	for {
		if err := ms.prep(ctx); err != nil {
			return err
		}

		// note: MessageType implements Stringer.
		logger.Debugf("[outbound rpc] writing fire-and-forget outbound message; to_peer=%s, type=%s, cid_key=%s, peer_key=%s, raw_key=%x, closer=%d, providers=%d, xor_common_zeros=%d, remote_addr=%s",
			ms.p, pmes.GetType(), c, pid, pmes.Key, len(pmes.GetCloserPeers()), len(pmes.ProviderPeers), distance, addr)

		startTime := time.Now()
		if err := ms.writeMsg(pmes); err != nil {
			ms.s.Reset()
			ms.s = nil

			elapsedTime := time.Since(startTime)
			latencyMillis := float64(elapsedTime) / float64(time.Millisecond)

			if retry {
				logger.Infof("[outbound rpc] error while writing fire-and-forget, bailing; err=%s, to_peer=%s, type=%s, cid_key=%s, peer_key=%s, raw_key=%x, closer=%d, providers=%d, elapsed_ms=%f, xor_common_zeros=%d, remote_addr=%s",
					err, ms.p, pmes.GetType(), c, pid, pmes.Key, len(pmes.GetCloserPeers()), len(pmes.ProviderPeers), latencyMillis, distance, addr)
				return err
			}
			logger.Infof("[outbound rpc] error while writing fire-and-forget, trying again; err=%s, to_peer=%s, type=%s, cid_key=%s, peer_key=%s, raw_key=%x, closer=%d, providers=%d, elapsed_ms=%f, xor_common_zeros=%d, remote_addr=%s",
				err, ms.p, pmes.GetType(), c, pid, pmes.Key, len(pmes.GetCloserPeers()), len(pmes.ProviderPeers), latencyMillis, distance, addr)

			retry = true
			continue
		}

		elapsedTime := time.Since(startTime)
		latencyMillis := float64(elapsedTime) / float64(time.Millisecond)

		// note: MessageType implements Stringer.
		logger.Debugf("[outbound rpc] wrote fire-and-forget outbound message; to_peer=%s, type=%s, cid_key=%s, peer_key=%s, raw_key=%x, closer=%d, providers=%d, elapsed_ms=%f, xor_common_zeros=%d, remote_addr=%s",
			ms.p, pmes.GetType(), c, pid, pmes.Key, len(pmes.GetCloserPeers()), len(pmes.ProviderPeers), latencyMillis, distance, addr)

		logger.Event(ctx, "dhtSentMessage", ms.dht.self, ms.p, pmes)

		if ms.singleMes > streamReuseTries {
			go helpers.FullClose(ms.s)
			ms.s = nil
		} else if retry {
			ms.singleMes++
		}

		return nil
	}
}

func (ms *messageSender) SendRequest(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	retry := false

	c, _ := cid.Cast(pmes.Key)
	pid, _ := peer.IDFromBytes(pmes.Key)

	distb := xor(keyspace.XORKeySpace.Key(pmes.Key).Bytes, keyspace.XORKeySpace.Key([]byte(ms.p)).Bytes)
	distance := keyspace.ZeroPrefixLen(distb)
	var addr multiaddr.Multiaddr
	if ms.s != nil {
		addr = ms.s.Conn().RemoteMultiaddr()
	}

	for {
		if err := ms.prep(ctx); err != nil {
			return nil, err
		}

		startTime := time.Now()
		// note: MessageType implements Stringer.
		logger.Debugf("[outbound rpc] writing request outbound message; to_peer=%s, type=%s, cid_key=%s, peer_key=%s, raw_key=%x, closer=%d, providers=%d, xor_common_zeros=%d, remote_addr=%s",
			ms.p, pmes.GetType(), c, pid, pmes.Key, len(pmes.GetCloserPeers()), len(pmes.ProviderPeers), distance, addr)

		if err := ms.writeMsg(pmes); err != nil {
			ms.s.Reset()
			ms.s = nil

			if retry {
				logger.Info("error writing message, bailing: ", err)
				return nil, err
			}
			logger.Info("error writing message, trying again: ", err)
			retry = true
			continue
		}

		elapsedTime := time.Since(startTime)
		latencyMillis := float64(elapsedTime) / float64(time.Millisecond)
		// note: MessageType implements Stringer.
		logger.Debugf("[outbound rpc] wrote request outbound message; to_peer=%s, type=%s, cid_key=%s, peer_key=%s, raw_key=%x, closer=%d, providers=%d, elapsed_ms=%f, xor_common_zeros=%d, remote_addr=%s",
			ms.p, pmes.GetType(), c, pid, pmes.Key, len(pmes.GetCloserPeers()), len(pmes.ProviderPeers), latencyMillis, distance, addr)

		startTime = time.Now()

		mes := new(pb.Message)
		if err := ms.ctxReadMsg(ctx, mes); err != nil {
			ms.s.Reset()
			ms.s = nil

			if retry {
				logger.Info("error reading message, bailing: ", err)
				return nil, err
			}
			logger.Info("error reading message, trying again: ", err)
			retry = true
			continue
		}

		elapsedTime = time.Since(startTime)
		latencyMillis = float64(elapsedTime) / float64(time.Millisecond)

		c, _ = cid.Cast(mes.Key)
		pid, _ = peer.IDFromBytes(pmes.Key)
		// note: MessageType implements Stringer.
		logger.Debugf("[outbound rpc] read response message; from_peer=%s, type=%s, cid_key=%s, peer_key=%s, raw_key=%x, closer=%d, providers=%d, elapsed_ms=%f, xor_common_zeros=%d, remote_addr=%s",
			ms.p, mes.GetType(), c, pid, mes.Key, len(mes.GetCloserPeers()), len(mes.ProviderPeers), latencyMillis, distance, addr)

		logger.Event(ctx, "dhtSentMessage", ms.dht.self, ms.p, pmes)

		if ms.singleMes > streamReuseTries {
			go helpers.FullClose(ms.s)
			ms.s = nil
		} else if retry {
			ms.singleMes++
		}

		return mes, nil
	}
}

func (ms *messageSender) writeMsg(pmes *pb.Message) error {
	return writeMsg(ms.s, pmes)
}

func (ms *messageSender) ctxReadMsg(ctx context.Context, mes *pb.Message) error {
	errc := make(chan error, 1)
	go func(r msgio.ReadCloser) {
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

func xor(a, b kbucket.ID) kbucket.ID {
	return kbucket.ID(util.XOR(a, b))
}
