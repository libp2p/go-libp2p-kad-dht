package dht

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
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
		d.log.LogAttrs(ctx, slog.LevelWarn, "error attaching stream to DHT service", slog.String("error", err.Error()))
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
			return err
		} else if data == nil {
			return nil // nil error, nil data -> graceful end
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
		resp, err := d.streamHandleMsg(ctx, slogger, s.Conn().RemotePeer(), req)
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
		// if the reader returns an end-of-file signal, exit gracefully
		if errors.Is(err, io.EOF) {
			return nil, nil
		}

		// log any other errors than stream resets
		if err.Error() != "stream reset" {
			slogger.LogAttrs(ctx, slog.LevelDebug, "error reading message", slog.String("error", err.Error()))
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
		slogger.LogAttrs(ctx, slog.LevelDebug, "error unmarshalling message", slog.String("error", err.Error()))

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

// streamHandleMsg handles the give protobuf message based on its type from the
// given remote peer.
func (d *DHT) streamHandleMsg(ctx context.Context, slogger *slog.Logger, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	ctx, span := tracer.Start(ctx, "DHT.streamHandleMsg")
	defer span.End()

	slogger.LogAttrs(ctx, slog.LevelDebug, "handling message")

	switch req.GetType() {
	case pb.Message_FIND_NODE:
		return d.handleFindPeer(ctx, remote, req)
	case pb.Message_PING:
		return d.handlePing(ctx, remote, req)
	}

	return nil, fmt.Errorf("can't handle received message: %s", req.GetType().String())
}

// streamWriteMsg sends the given message over the stream.
func (d *DHT) streamWriteMsg(ctx context.Context, slogger *slog.Logger, s network.Stream, msg *pb.Message) error {
	ctx, span := tracer.Start(ctx, "DHT.streamWriteMsg")
	defer span.End()

	if err := writeMsg(s, msg); err != nil {
		slogger.LogAttrs(ctx, slog.LevelDebug, "error writing response", slog.String("error", err.Error()))
		stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
		return err
	}

	return nil
}
