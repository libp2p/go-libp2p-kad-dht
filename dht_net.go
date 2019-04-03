package dht

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"

	ggio "github.com/gogo/protobuf/io"
	ctxio "github.com/jbenet/go-context/io"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
)

var ErrReadTimeout = fmt.Errorf("timed out reading response")

type bufferedWriteCloser interface {
	ggio.WriteCloser
	Flush() error
}

// The Protobuf writer performs multiple small writes when writing a message.
// We need to buffer those writes, to make sure that we're not sending a new
// packet for every single write.
type bufferedDelimitedWriter struct {
	*bufio.Writer
	ggio.WriteCloser
}

func newBufferedDelimitedWriter(str io.Writer) bufferedWriteCloser {
	w := bufio.NewWriter(str)
	return &bufferedDelimitedWriter{
		Writer:      w,
		WriteCloser: ggio.NewDelimitedWriter(w),
	}
}

func (w *bufferedDelimitedWriter) Flush() error {
	return w.Writer.Flush()
}

// handleNewStream implements the inet.StreamHandler
func (dht *IpfsDHT) handleNewStream(s inet.Stream) {
	defer s.Reset()
	if dht.handleNewMessage(s) {
		// Gracefully close the stream for writes.
		s.Close()
	}
}

// Returns true on orderly completion of writes (so we can Close the stream).
func (dht *IpfsDHT) handleNewMessage(s inet.Stream) bool {
	ctx := dht.Context()
	cr := ctxio.NewReader(ctx, s) // ok to use. we defer close stream in this func
	cw := ctxio.NewWriter(ctx, s) // ok to use. we defer close stream in this func
	r := ggio.NewDelimitedReader(cr, inet.MessageSizeMax)
	w := newBufferedDelimitedWriter(cw)
	mPeer := s.Conn().RemotePeer()

	for {
		var req pb.Message
		switch err := r.ReadMsg(&req); err {
		case io.EOF:
			return true
		default:
			// This string test is necessary because there isn't a single stream reset error
			// instance	in use.
			if err.Error() != "stream reset" {
				logger.Debugf("error reading message: %#v", err)
			}
			return false
		case nil:
		}

		startedHandling := time.Now()

		receivedMessages.WithLabelValues(dht.messageLabelValues(&req)...).Inc()
		receivedMessageSizeBytes.WithLabelValues(dht.messageLabelValues(&req)...).Observe(float64(req.Size()))

		handler := dht.handlerForMsgType(req.GetType())
		if handler == nil {
			logger.Warningf("can't handle received message of type %v", req.GetType())
			return false
		}

		resp, err := handler(ctx, mPeer, &req)
		if err != nil {
			logger.Debugf("error handling message: %v", err)
			return false
		}

		dht.updateFromMessage(ctx, mPeer, &req)

		if resp == nil {
			continue
		}

		// send out response msg
		err = w.WriteMsg(resp)
		if err == nil {
			err = w.Flush()
		}
		if err != nil {
			logger.Debugf("error writing response: %v", err)
			return false
		}
		inboundRequestHandlingTimeSeconds.WithLabelValues(dht.messageLabelValues(&req)...).Observe(time.Since(startedHandling).Seconds())
	}
}

func (dht *IpfsDHT) newNetStream(ctx context.Context, p peer.ID) (inet.Stream, error) {
	t := time.Now()
	s, err := dht.host.NewStream(ctx, p, dht.protocols...)
	newStreamTimeSeconds.WithLabelValues(errorLabelValue(err)).Observe(time.Since(t).Seconds())
	return s, err
}

func (dht *IpfsDHT) observeRpcLatency(sent *pb.Message, rpcType string, err *error, started time.Time) {
	outboundRpcLatencySeconds.WithLabelValues(
		sent.Type.String(), rpcType, errorLabelValue(*err),
	).Observe(time.Since(started).Seconds())
}

// sendRequest sends out a request, but also makes sure to measure the RTT for latency measurements.
func (dht *IpfsDHT) sendRequest(ctx context.Context, p peer.ID, req *pb.Message) (_ *pb.Message, err error) {
	dht.recordOutboundMessage(ctx, req)
	started := time.Now()
	defer dht.observeRpcLatency(req, "request", &err, started)
	reply, err := dht.streamPool.getPeer(p).doRequest(ctx, req)
	if err == nil {
		dht.updateFromMessage(ctx, p, reply)
		dht.peerstore.RecordLatency(p, time.Since(started))
	}
	return reply, err
}

// sendMessage sends out a message
func (dht *IpfsDHT) sendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) (err error) {
	dht.recordOutboundMessage(ctx, pmes)
	started := time.Now()
	defer dht.observeRpcLatency(pmes, "send", &err, started)
	return dht.streamPool.getPeer(p).send(ctx, pmes)
}

func (dht *IpfsDHT) recordOutboundMessage(ctx context.Context, m *pb.Message) {
	lvs := dht.messageLabelValues(m)
	sentMessages.WithLabelValues(lvs...).Inc()
	sentMessageSizeBytes.WithLabelValues(lvs...).Observe(float64(m.Size()))
}

func (dht *IpfsDHT) updateFromMessage(ctx context.Context, p peer.ID, mes *pb.Message) error {
	// Make sure that this node is actually a DHT server, not just a client.
	protos, err := dht.peerstore.SupportsProtocols(p, dht.protocolStrs()...)
	if err == nil && len(protos) > 0 {
		dht.Update(ctx, p)
	}
	return nil
}
