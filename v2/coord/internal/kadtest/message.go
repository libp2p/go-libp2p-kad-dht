package kadtest

import (
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/key"
)

// StrAddr is a simple implementation of kad.Address that uses a string to represent the address.
type StrAddr string

var _ kad.Address[StrAddr] = StrAddr("")

func (a StrAddr) Equal(b StrAddr) bool { return a == b }

type Request[K kad.Key[K]] struct {
	target K
	id     string
}

func NewRequest[K kad.Key[K]](id string, target K) *Request[K] {
	return &Request[K]{
		target: target,
		id:     id,
	}
}

func (r *Request[K]) Target() K {
	return r.target
}

func (r *Request[K]) ID() string {
	return r.id
}

func (r *Request[K]) EmptyResponse() kad.Response[K, StrAddr] {
	return &Response[K]{}
}

type Response[K kad.Key[K]] struct {
	id     string
	closer []kad.NodeInfo[K, StrAddr]
}

func NewResponse[K kad.Key[K]](id string, closer []kad.NodeInfo[K, StrAddr]) *Response[K] {
	return &Response[K]{
		id:     id,
		closer: closer,
	}
}

func (r *Response[K]) ID() string {
	return r.id
}

func (r *Response[K]) CloserNodes() []kad.NodeInfo[K, StrAddr] {
	return r.closer
}

type (
	// Request8 is a Request message that uses key.Key8
	Request8 = Request[key.Key8]

	// Response8 is a Response message that uses key.Key8
	Response8 = Response[key.Key8]

	// Request8 is a Request message that uses key.Key256
	Request256 = Request[key.Key256]

	// Response256 is a Response message that uses key.Key256
	Response256 = Response[key.Key256]
)

var (
	_ kad.Request[key.Key8, StrAddr]  = (*Request8)(nil)
	_ kad.Response[key.Key8, StrAddr] = (*Response8)(nil)
)

var (
	_ kad.Request[key.Key256, StrAddr]  = (*Request256)(nil)
	_ kad.Response[key.Key256, StrAddr] = (*Response256)(nil)
)
