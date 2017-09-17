package dht

import (
	"context"
	"fmt"
	"sync"
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
	defer s.Close()

	ctx := dht.Context()
	cr := ctxio.NewReader(ctx, s) // ok to use. we defer close stream in this func
	cw := ctxio.NewWriter(ctx, s) // ok to use. we defer close stream in this func
	r := ggio.NewDelimitedReader(cr, inet.MessageSizeMax)
	w := ggio.NewDelimitedWriter(cw)
	mPeer := s.Conn().RemotePeer()

	for {
		// receive msg
		pmes := new(pb.Message)
		if err := r.ReadMsg(pmes); err != nil {
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

// sendRequest sends out a request, but also makes sure to
// measure the RTT for latency measurements.
func (dht *IpfsDHT) sendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {

	ms, err := dht.messageSenderForPeer(p)
	if err != nil {
		return nil, err
	}

	start := time.Now()

	rpmes, err := ms.SendRequest(ctx, pmes)
	if err != nil {
		return nil, err
	}

	// update the peer (on valid msgs only)
	dht.updateFromMessage(ctx, p, rpmes)

	dht.peerstore.RecordLatency(p, time.Since(start))
	log.Event(ctx, "dhtReceivedMessage", dht.self, p, rpmes)
	return rpmes, nil
}

// sendMessage sends out a message
func (dht *IpfsDHT) sendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	ms, err := dht.messageSenderForPeer(p)
	if err != nil {
		return err
	}

	if err := ms.SendMessage(ctx, pmes); err != nil {
		return err
	}
	log.Event(ctx, "dhtSentMessage", dht.self, p, pmes)
	return nil
}

func (dht *IpfsDHT) updateFromMessage(ctx context.Context, p peer.ID, mes *pb.Message) error {
	dht.Update(ctx, p)
	return nil
}

func (dht *IpfsDHT) messageSenderForPeer(p peer.ID) (*messageSender, error) {
	dht.smlk.Lock()
	ms, ok := dht.strmap[p]
	if ok {
		dht.smlk.Unlock()
		return ms, nil
	}
	ms = &messageSender{p: p, dht: dht}
	dht.strmap[p] = ms
	dht.smlk.Unlock()

	if err := ms.prepOrInvalidate(); err != nil {
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
	s      inet.Stream
	w      ggio.WriteCloser
	rch    chan chan requestResult
	rcount int
	lk     sync.Mutex
	p      peer.ID
	dht    *IpfsDHT

	invalid   bool
	singleMes int
}

type requestResult struct {
	mes *pb.Message
	err error
}

const requestResultBuffer = 16

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

func (ms *messageSender) prepOrInvalidate() error {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	if err := ms.prep(); err != nil {
		ms.invalidate()
		return err
	}
	return nil
}

func (ms *messageSender) prep() error {
	if ms.invalid {
		return fmt.Errorf("message sender has been invalidated")
	}
	if ms.s != nil {
		return nil
	}

	nstr, err := ms.dht.host.NewStream(ms.dht.ctx, ms.p, ProtocolDHT, ProtocolDHTOld)
	if err != nil {
		return err
	}

	r := ggio.NewDelimitedReader(nstr, inet.MessageSizeMax)
	rch := make(chan chan requestResult, requestResultBuffer)
	go messageReceiver(ms.dht.ctx, rch, r)

	ms.rch = rch
	ms.w = ggio.NewDelimitedWriter(nstr)
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
	for {
		if ms.singleMes > streamReuseTries {
			// TODO do this without holding the lock
			return ms.sendMessageSingle(ctx, pmes)
		}

		if err := ms.prep(); err != nil {
			return err
		}

		if err := ms.w.WriteMsg(pmes); err != nil {
			ms.resetHard()

			if retry {
				log.Info("error writing message, bailing: ", err)
				return err
			} else {
				log.Info("error writing message, trying again: ", err)
				retry = true
				continue
			}
		}

		log.Event(ctx, "dhtSentMessage", ms.dht.self, ms.p, pmes)

		if ms.singleMes > streamReuseTries {
			ms.resetHard()
		} else if retry {
			ms.singleMes++
		}

		return nil
	}
}

func (ms *messageSender) SendRequest(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
	// XXX log total request time
	retry := false
	for {
		ms.lk.Lock()

		if ms.singleMes > streamReuseTries {
			ms.lk.Unlock()
			return ms.sendRequestSingle(ctx, pmes)
		}

		if err := ms.prep(); err != nil {
			ms.lk.Unlock()
			return nil, err
		}

		if err := ms.w.WriteMsg(pmes); err != nil {
			ms.resetHard()
			ms.lk.Unlock()

			if retry {
				log.Info("error writing message, bailing: ", err)
				return nil, err
			} else {
				log.Info("error writing message, trying again: ", err)
				retry = true
				continue
			}
		}

		// XXX log

		resch := make(chan requestResult, 1)
		select {
		case ms.rch <- resch:
		default:
			// XXX Implement me -- pipeline stall
		}

		rcount := ms.rcount

		ms.lk.Unlock()

		var res requestResult
		select {
		case res = <-resch:

		case <-ctx.Done():
			return nil, ctx.Err()

			// XXX read timeout
		}

		if res.err != nil {
			ms.lk.Lock()
			ms.resetSoft(rcount)
			ms.lk.Unlock()

			if retry {
				log.Info("error reading message, bailing: ", res.err)
				return nil, res.err
			} else {
				log.Info("error reading message, trying again: ", res.err)
				retry = true
				continue
			}
		}

		// XXX log

		ms.lk.Lock()
		if ms.singleMes > streamReuseTries {
			ms.resetSoft(rcount)
		} else if retry {
			ms.singleMes++
		}
		ms.lk.Unlock()

		return res.mes, nil
	}
}

func (ms *messageSender) sendMessageSingle(ctx context.Context, pmes *pb.Message) error {
	s, err := ms.dht.host.NewStream(ctx, ms.p, ProtocolDHT, ProtocolDHTOld)
	if err != nil {
		return err
	}
	defer s.Close()

	w := ggio.NewDelimitedWriter(s)
	return w.WriteMsg(pmes)
}

func (ms *messageSender) sendRequestSingle(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
	s, err := ms.dht.host.NewStream(ctx, ms.p, ProtocolDHT, ProtocolDHTOld)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	r := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
	w := ggio.NewDelimitedWriter(s)

	if err := w.WriteMsg(pmes); err != nil {
		return nil, err
	}

	mes := new(pb.Message)

	// XXX context, timeout
	if err := r.ReadMsg(mes); err != nil {
		return nil, err
	}

	return mes, nil
}

// Resets the stream unconditionally; increments the reset count.
// Mutex must be locked.
func (ms *messageSender) resetHard() {
	close(ms.rch)
	ms.s.Reset()
	ms.s = nil
	ms.rcount++
}

// Resets the stream only if the reset count matches the argument
// Allows multiple read failures in batched concurrent requests with
// only a single reset between them.
// Mutex must be locked.
func (ms *messageSender) resetSoft(rcount int) {
	if rcount != ms.rcount {
		return
	}

	ms.resetHard()
}

func messageReceiver(ctx context.Context, rch chan chan requestResult, r ggio.ReadCloser) {
	// XXX Implement me
}
