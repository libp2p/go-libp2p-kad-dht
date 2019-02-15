package dht

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	ggio "github.com/gogo/protobuf/io"
	ctxio "github.com/jbenet/go-context/io"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	"golang.org/x/xerrors"
)

var dhtReadMessageTimeout = time.Minute
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
	if dht.client {
		return
	}
	dht.handleNewMessage(s)
}

func (dht *IpfsDHT) handleNewMessage(s inet.Stream) error {
	ctx := dht.Context()
	cr := ctxio.NewReader(ctx, s) // ok to use. we defer close stream in this func
	cw := ctxio.NewWriter(ctx, s) // ok to use. we defer close stream in this func
	r := ggio.NewDelimitedReader(cr, inet.MessageSizeMax)
	w := newBufferedDelimitedWriter(cw)
	mPeer := s.Conn().RemotePeer()

	for {
		var pmes pb.Message
		switch err := r.ReadMsg(&pmes); err {
		case io.EOF:
			return nil
		default:
			if err.Error() != "stream reset" {
				logger.Debugf("error reading message: %#v", err)
			}
			return err
		case nil:
		}

		handler := dht.handlerForMsgType(pmes.GetType())
		if handler == nil {
			logger.Warningf("can't handle received message of type %s", pmes.GetType())
			return nil
		}

		// dispatch handler.
		rpmes, err := handler(ctx, mPeer, &pmes)
		if err != nil {
			logger.Debugf("handle message error: %s", err)
			return err
		}

		// if nil response, return it before serializing
		if rpmes == nil {
			logger.Debug("got back nil response from request")
			continue
		}

		// send out response msg
		err = w.WriteMsg(rpmes)
		if err == nil {
			err = w.Flush()
		}
		if err != nil {
			logger.Debugf("send response error: %s", err)
			return nil
		}

		// update the peer (on valid msgs only)
		dht.updateFromMessage(ctx, mPeer, &pmes)
	}
}

// sendRequest sends out a request, but also makes sure to
// measure the RTT for latency measurements.
func (dht *IpfsDHT) sendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	defer func(started time.Time) {
		log.Printf("time taken to send request: %v: err=%v", time.Since(started), err)
	}(time.Now())
	s, err := dht.newStream(ctx, p)
	if err != nil {
		return nil, xerrors.Errorf("error creating new stream: %w", err)
	}
	defer s.Reset()
	dr := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
	bdw := newBufferedDelimitedWriter(s)
	start := time.Now()
	err = bdw.WriteMsg(pmes)
	if err != nil {
		return nil, xerrors.Errorf("error writing message: %w", err)
	}
	if err := bdw.Flush(); err != nil {
		return nil, xerrors.Errorf("error flushing message: %w", err)
	}
	var reply pb.Message
	if err := dr.ReadMsg(&reply); err != nil {
		return nil, xerrors.Errorf("error reading reply: %w", err)
	}
	// update the peer (on valid msgs only)
	dht.updateFromMessage(ctx, p, &reply)
	dht.peerstore.RecordLatency(p, time.Since(start))
	return &reply, nil
}

func (dht *IpfsDHT) newStream(ctx context.Context, p peer.ID) (inet.Stream, error) {
	return dht.host.NewStream(ctx, p, dht.protocols...)
}

// sendMessage sends out a message
func (dht *IpfsDHT) sendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) (err error) {
	defer func(started time.Time) {
		log.Printf("time taken to send message: %v: err=%v", time.Since(started), err)
	}(time.Now())
	s, err := dht.newStream(ctx, p)
	if err != nil {
		return xerrors.Errorf("error creating new stream: %w", err)
	}
	defer s.Reset()
	bdw := newBufferedDelimitedWriter(s)
	err = bdw.WriteMsg(pmes)
	if err != nil {
		return xerrors.Errorf("error writing message: %w", err)
	}
	if err := bdw.Flush(); err != nil {
		return xerrors.Errorf("error flushing message: %w", err)
	}
	return nil
}

func (dht *IpfsDHT) updateFromMessage(ctx context.Context, p peer.ID, mes *pb.Message) error {
	// Make sure that this node is actually a DHT server, not just a client.
	protos, err := dht.peerstore.SupportsProtocols(p, dht.protocolStrs()...)
	if err == nil && len(protos) > 0 {
		dht.Update(ctx, p)
	}
	return nil
}
