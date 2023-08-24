package dht

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"github.com/libp2p/go-msgio/protoio"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/metrics"
	pb "github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

// streamHandler is the function that's registered with the libp2p host for
// the DHT protocol ID. It sets up metrics and the resource manager scope. It
// actually starts handling the stream and depending on the outcome resets or
// closes it.
func (d *DHT) streamHandler(s network.Stream) {
	ctx, _ := tag.New(context.Background(),
		tag.Upsert(metrics.KeyPeerID, d.host.ID().String()),
		tag.Upsert(metrics.KeyInstanceID, fmt.Sprintf("%p", d)),
	)

	if err := s.Scope().SetService(ServiceName); err != nil {
		d.log.LogAttrs(ctx, slog.LevelWarn, "error attaching stream to DHT service", slog.String("err", err.Error()))
		_ = s.Reset()
		return
	}

	if err := d.handleNewStream(ctx, s); err != nil {
		// If we exited with an error, let the remote peer know.
		_ = s.Reset()
	} else {
		// If we exited without an error, close gracefully.
		_ = s.Close()
	}
}

// handleNewStream is a method associated with the DHT type. This function
// handles the incoming stream from a remote peer.
//
// This function goes through the following steps:
//  1. Starts a new trace span for the stream handling operation.
//  2. Sets an idle timeout for the stream doing the operation.
//  3. Reads messages from the stream in a loop.
//  4. If a message is received, it starts a timer, and unmarshals the message.
//  5. If the message unmarshals successfully, it resets the stream deadline,
//     tags the context with the message type and key, updates some metrics and
//     then handles the message and gathers the response from the handler.
//  6. If responding is needed, sends the response back to the peer.
//  7. Logs the latency and updates the relevant metrics before the loop iterates.
//
// Returns:
// It returns an error if any of the operations in the pipeline fails, otherwise
// it will return nil indicating the end of the stream or all messages have been
// processed correctly.
func (d *DHT) handleNewStream(ctx context.Context, s network.Stream) error {
	ctx, span := tracer.Start(ctx, "DHT.handleNewStream")
	defer span.End()

	// init structured logger that always contains the remote peers PeerID
	slogger := d.log.With(slog.String("from", s.Conn().RemotePeer().String()))

	// reset the stream after it was idle for too long
	if err := s.SetDeadline(time.Now().Add(d.cfg.TimeoutStreamIdle)); err != nil {
		return fmt.Errorf("set initial stream deadline: %w", err)
	}

	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
	for {
		// 1. read message from stream
		data, err := d.streamReadMsg(ctx, slogger, reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		// we have received a message, start the timer to
		// track inbound request latency
		startTime := time.Now()

		// 2. unmarshal message into something usable
		req, err := d.streamUnmarshalMsg(ctx, slogger, data)
		if err != nil {
			return err
		}

		// signal to buffer pool that it can reuse the bytes
		reader.ReleaseMsg(data)

		// reset stream deadline
		if err = s.SetDeadline(time.Now().Add(d.cfg.TimeoutStreamIdle)); err != nil {
			return fmt.Errorf("reset stream deadline: %w", err)
		}

		// extend metrics context and slogger with message information.
		// ctx must be overwritten because in the next iteration metrics.KeyMessageType
		// would already exist and tag.New would return an error.
		ctx, _ := tag.New(ctx, tag.Upsert(metrics.KeyMessageType, req.GetType().String()))
		slogger = slogger.With(
			slog.String("type", req.GetType().String()),
			slog.String("key", base64.StdEncoding.EncodeToString(req.GetKey())),
		)

		// track message metrics
		stats.Record(ctx,
			metrics.ReceivedMessages.M(1),
			metrics.ReceivedBytes.M(int64(len(data))),
		)

		// 3. handle the message and gather response
		slogger.LogAttrs(ctx, slog.LevelDebug, "handling message")
		resp, err := d.handleMsg(ctx, s.Conn().RemotePeer(), req)
		if err != nil {
			slogger.LogAttrs(ctx, slog.LevelDebug, "error handling message", slog.Duration("time", time.Since(startTime)), slog.String("error", err.Error()))
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			return err
		}
		slogger.LogAttrs(ctx, slog.LevelDebug, "handled message", slog.Duration("time", time.Since(startTime)))

		// if the handler didn't return a response, continue reading the stream
		if resp == nil {
			continue
		}

		// 4. sent remote peer our response
		err = d.streamWriteMsg(ctx, slogger, s, resp)
		if err != nil {
			return err
		}

		// final logging, metrics tracking
		latency := time.Since(startTime)
		slogger.LogAttrs(ctx, slog.LevelDebug, "responded to message", slog.Duration("time", latency))
		stats.Record(ctx, metrics.InboundRequestLatency.M(float64(latency.Milliseconds())))
	}
}

// streamReadMsg reads a message from the given msgio.Reader and returns the
// corresponding bytes. If an error occurs it, logs it, and updates the metrics.
// If the bytes are empty and the error is nil, the remote peer returned
func (d *DHT) streamReadMsg(ctx context.Context, slogger *slog.Logger, r msgio.Reader) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "DHT.streamReadMsg")
	defer span.End()

	data, err := r.ReadMsg()
	if err != nil {
		// log any other errors than stream resets
		if !errors.Is(err, network.ErrReset) {
			slogger.LogAttrs(ctx, slog.LevelDebug, "error reading message", slog.String("err", err.Error()))
		}

		// record any potential partial message we have received
		if len(data) > 0 {
			_ = stats.RecordWithTags(ctx,
				[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
				metrics.ReceivedMessages.M(1),
				metrics.ReceivedMessageErrors.M(1),
				metrics.ReceivedBytes.M(int64(len(data))),
			)
		}

		return nil, err
	}

	return data, nil
}

// streamUnmarshalMsg takes the byte slice and tries to unmarshal it into a
// protobuf message. If an error occurs, it will be logged and the metrics will
// be updated.
func (d *DHT) streamUnmarshalMsg(ctx context.Context, slogger *slog.Logger, data []byte) (*pb.Message, error) {
	ctx, span := tracer.Start(ctx, "DHT.streamUnmarshalMsg")
	defer span.End()

	var req pb.Message
	if err := req.Unmarshal(data); err != nil {
		slogger.LogAttrs(ctx, slog.LevelDebug, "error unmarshalling message", slog.String("err", err.Error()))

		_ = stats.RecordWithTags(ctx,
			[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
			metrics.ReceivedMessages.M(1),
			metrics.ReceivedMessageErrors.M(1),
			metrics.ReceivedBytes.M(int64(len(data))),
		)

		return nil, err
	}

	return &req, nil
}

// handleMsg handles the give protobuf message based on its type from the
// given remote peer.
func (d *DHT) handleMsg(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	ctx, span := tracer.Start(ctx, "DHT.handle_"+req.GetType().String())
	defer span.End()

	switch req.GetType() {
	case pb.Message_FIND_NODE:
		return d.handleFindPeer(ctx, remote, req)
	case pb.Message_PING:
		return d.handlePing(ctx, remote, req)
	case pb.Message_PUT_VALUE:
		return d.handlePutValue(ctx, remote, req)
	case pb.Message_GET_VALUE:
		return d.handleGetValue(ctx, remote, req)
	case pb.Message_ADD_PROVIDER:
		return d.handleAddProvider(ctx, remote, req)
	case pb.Message_GET_PROVIDERS:
		return d.handleGetProviders(ctx, remote, req)
	default:
		return nil, fmt.Errorf("can't handle received message: %s", req.GetType().String())
	}
}

// streamWriteMsg sends the given message over the stream and handles traces
// and telemetry.
func (d *DHT) streamWriteMsg(ctx context.Context, slogger *slog.Logger, s network.Stream, msg *pb.Message) error {
	ctx, span := tracer.Start(ctx, "DHT.streamWriteMsg")
	defer span.End()

	if err := writeMsg(s, msg); err != nil {
		slogger.LogAttrs(ctx, slog.LevelDebug, "error writing response", slog.String("err", err.Error()))
		stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
		return err
	}

	return nil
}

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
