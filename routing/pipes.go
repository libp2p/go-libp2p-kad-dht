package routing

import (
	"bytes"
	"errors"
	"github.com/libp2p/go-libp2p-core/peer"
	record "github.com/libp2p/go-libp2p-record"
)

type Processor interface {
	Process(interface{}, func()) (interface{}, error)
}

var skipErr = errors.New("skip value")

type CountStopper struct {
	Count    int
	MaxCount int
}

func (f *CountStopper) Process(val interface{}, abortQuery func()) (interface{}, error) {
	f.Count++
	if f.MaxCount > 0 && f.Count >= f.MaxCount {
		abortQuery()
	}
	return val, nil
}

type ValidationFilter struct {
	Key       string
	Validator record.Validator
}

func (f *ValidationFilter) Process(val interface{}, _ func()) (interface{}, error) {
	v := val.(RecvdVal)
	err := f.Validator.Validate(f.Key, v.Val)
	if err != nil {
		return nil, err
	}
	return v, nil
}

type BestValueFilterRecorder struct {
	Key           string
	Best          []byte
	Validator     record.Validator
	PeersWithBest map[peer.ID]struct{}
}

func (p *BestValueFilterRecorder) Process(val interface{}, _ func()) (interface{}, error) {
	v := val.(RecvdVal)

	// Select best value
	if p.Best != nil {
		if bytes.Equal(p.Best, v.Val) {
			p.PeersWithBest[v.From] = struct{}{}
			return nil, skipErr
		}
		newIsBetter, err := p.getBetterRecord(p.Key, p.Best, v.Val)
		if err != nil {
			logger.Warnw("failed to select best value", "key", p.Key, "error", err)
			return nil, skipErr
		}
		if !newIsBetter {
			return nil, skipErr
		}
	}
	p.PeersWithBest = make(map[peer.ID]struct{})
	p.PeersWithBest[v.From] = struct{}{}
	p.Best = v.Val
	return v, nil
}

func (p *BestValueFilterRecorder) getBetterRecord(key string, current, new []byte) (bool, error) {
	sel, err := p.Validator.Select(key, [][]byte{current, new})
	if err != nil {
		return false, err
	}
	return sel == 1, nil
}

type NewPeerIDFilter struct {
	Key   string
	Peers *peer.Set
}

func (p *NewPeerIDFilter) Process(val interface{}, _ func()) (interface{}, error) {
	prov := val.(peer.AddrInfo)

	logger.Debugf("got provider: %s", prov)
	if p.Peers.TryAdd(prov.ID) {
		logger.Debugf("using provider: %s", prov)
		return prov, nil
	}

	return nil, skipErr
}
