package dht

import (
	"bufio"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-msgio/protoio"

	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"

	"github.com/libp2p/go-msgio"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
)

var dhtReadMessageTimeout = 10 * time.Second
var dhtStreamIdleTimeout = 1 * time.Minute

// ErrReadTimeout is an error that occurs when no message is read within the timeout period.
var ErrReadTimeout = fmt.Errorf("timed out reading response")

// The Protobuf writer performs multiple small writes when writing a message.
// We need to buffer those writes, to make sure that we're not sending a new
// packet for every single write.
type bufferedDelimitedWriter struct {
	*bufio.Writer
	protoio.WriteCloser
}

var writerPool = sync.Pool{
	New: func() interface{} {
		w := bufio.NewWriter(nil)
		return &bufferedDelimitedWriter{
			Writer:      w,
			WriteCloser: protoio.NewDelimitedWriter(w),
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
	if dht.handleNewMessage(s) {
		// If we exited without error, close gracefully.
		_ = s.Close()
	} else {
		// otherwise, send an error.
		_ = s.Reset()
	}
}

// Returns true on orderly completion of writes (so we can Close the stream).
func (dht *IpfsDHT) handleNewMessage(s network.Stream) bool {
	ctx := dht.ctx
	r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	mPeer := s.Conn().RemotePeer()

	timer := time.AfterFunc(dhtStreamIdleTimeout, func() { _ = s.Reset() })
	defer timer.Stop()

	for {
		if dht.getMode() != modeServer {
			logger.Errorf("ignoring incoming dht message while not in server mode")
			return false
		}

		var req pb.Message
		msgbytes, err := r.ReadMsg()
		msgLen := len(msgbytes)
		if err != nil {
			r.ReleaseMsg(msgbytes)
			if err == io.EOF {
				return true
			}
			// This string test is necessary because there isn't a single stream reset error
			// instance	in use.
			if c := baseLogger.Check(zap.DebugLevel, "error reading message"); c != nil && err.Error() != "stream reset" {
				c.Write(zap.String("from", mPeer.String()),
					zap.Error(err))
			}
			if msgLen > 0 {
				_ = stats.RecordWithTags(ctx,
					[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
					metrics.ReceivedMessages.M(1),
					metrics.ReceivedMessageErrors.M(1),
					metrics.ReceivedBytes.M(int64(msgLen)),
				)
			}
			return false
		}
		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			if c := baseLogger.Check(zap.DebugLevel, "error unmarshaling message"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Error(err))
			}
			_ = stats.RecordWithTags(ctx,
				[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
				metrics.ReceivedMessages.M(1),
				metrics.ReceivedMessageErrors.M(1),
				metrics.ReceivedBytes.M(int64(msgLen)),
			)
			return false
		}

		timer.Reset(dhtStreamIdleTimeout)

		startTime := time.Now()
		ctx, _ := tag.New(ctx,
			tag.Upsert(metrics.KeyMessageType, req.GetType().String()),
		)

		stats.Record(ctx,
			metrics.ReceivedMessages.M(1),
			metrics.ReceivedBytes.M(int64(msgLen)),
		)

		handler := dht.handlerForMsgType(req.GetType())
		if handler == nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			if c := baseLogger.Check(zap.DebugLevel, "can't handle received message"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())))
			}
			return false
		}

		// a peer has queried us, let's add it to RT
		dht.peerFound(dht.ctx, mPeer, true)

		if c := baseLogger.Check(zap.DebugLevel, "handling message"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()))
		}
		resp, err := handler(ctx, mPeer, &req)
		if err != nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			if c := baseLogger.Check(zap.DebugLevel, "error handling message"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())),
					zap.Binary("key", req.GetKey()),
					zap.Error(err))
			}
			return false
		}

		if c := baseLogger.Check(zap.DebugLevel, "handled message"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()),
				zap.Duration("time", time.Since(startTime)))
		}

		if resp == nil {
			continue
		}

		// send out response msg
		err = writeMsg(s, resp)
		if err != nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			if c := baseLogger.Check(zap.DebugLevel, "error writing response"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())),
					zap.Binary("key", req.GetKey()),
					zap.Error(err))
			}
			return false
		}

		elapsedTime := time.Since(startTime)

		if c := baseLogger.Check(zap.DebugLevel, "responded to message"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()),
				zap.Duration("time", elapsedTime))
		}

		latencyMillis := float64(elapsedTime) / float64(time.Millisecond)
		stats.Record(ctx, metrics.InboundRequestLatency.M(latencyMillis))
	}
}
