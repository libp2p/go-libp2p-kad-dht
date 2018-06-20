package dht

import (
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

var dhtReadMessageTimeout = time.Minute
var ErrReadTimeout = fmt.Errorf("timed out reading response")

// handleNewStream implements the inet.StreamHandler
func (dht *IpfsDHT) handleNewStream(s inet.Stream) {
	go dht.handleNewMessage(s)
}

func (dht *IpfsDHT) handleNewMessage(s inet.Stream) {
	ctx := dht.Context()
	cr := ctxio.NewReader(ctx, s) // ok to use. we defer close stream in this func
	cw := ctxio.NewWriter(ctx, s) // ok to use. we defer close stream in this func
	r := ggio.NewDelimitedReader(cr, inet.MessageSizeMax)
	w := ggio.NewDelimitedWriter(cw)
	mPeer := s.Conn().RemotePeer()

	for {
		// receive msg
		pmes := new(pb.Message)
		switch err := r.ReadMsg(pmes); err {
		case io.EOF:
			s.Close()
			return
		case nil:
		default:
			s.Reset()
			log.Debugf("Error unmarshaling data: %s", err)
			return
		}

		// update the peer (on valid msgs only)
		dht.updateFromMessage(ctx, mPeer, pmes)

		// get handler for this msg type.
		handler := dht.handlerForMsgType(pmes.GetType())
		if handler == nil {
			s.Reset()
			log.Debug("got back nil handler from handlerForMsgType")
			return
		}

		// dispatch handler.
		rpmes, err := handler(ctx, mPeer, pmes)
		if err != nil {
			s.Reset()
			log.Debugf("handle message error: %s", err)
			return
		}

		// if nil response, return it before serializing
		if rpmes == nil {
			log.Debug("got back nil response from request")
			continue
		}

		// send out response msg
		if err := w.WriteMsg(rpmes); err != nil {
			s.Reset()
			log.Debugf("send response error: %s", err)
			return
		}
	}
}

func (dht *IpfsDHT) updateFromMessage(ctx context.Context, p peer.ID, mes *pb.Message) error {
	dht.Update(ctx, p)
	return nil
}

func (dht *IpfsDHT) sendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	log.Event(ctx, "dhtSentMessage", dht.self, p, pmes)

	s, err := dht.host.NewStream(ctx, p, dht.protocols...)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		w := ggio.NewDelimitedWriter(s)
		if err := w.WriteMsg(pmes); err != nil {
			s.Reset()
			errCh <- err
			return
		}
		close(errCh)
		// Ideally, we'd wait. However, we may end up waiting forever.
		// TODO: *do* wait after we fix shit.
		inet.FullClose(s)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.Reset()
		return ctx.Err()
	}
}

func (dht *IpfsDHT) sendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	start := time.Now()

	s, err := dht.host.NewStream(ctx, p, dht.protocols...)
	if err != nil {
		return nil, err
	}

	errCh := make(chan error, 1)
	mes := new(pb.Message)
	go func() {
		r := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
		w := ggio.NewDelimitedWriter(s)
		if err := w.WriteMsg(pmes); err != nil {
			s.Reset()
			errCh <- err
			return
		}
		// TODO: Close immediately after writing. Don't do this yet as
		// some peers running old software will misbehave if the stream
		// gets half-closed.
		if err := r.ReadMsg(mes); err != nil {
			s.Reset()
			errCh <- err
			return
		}
		close(errCh)
		inet.FullClose(s)
	}()
	select {
	case err, ok := <-errCh:
		if ok {
			return nil, err
		}

		// update the peer (on valid msgs only)
		dht.updateFromMessage(ctx, p, mes)
		dht.peerstore.RecordLatency(p, time.Since(start))
		log.Event(ctx, "dhtReceivedMessage", dht.self, p, mes)
		return mes, nil
	case <-ctx.Done():
		s.Reset()
		return nil, ctx.Err()
	}
}
