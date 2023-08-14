package dht

import (
	"github.com/plprobelab/go-kademlia/kad"
	"github.com/plprobelab/go-kademlia/network/address"
)

var protoID = address.ProtocolID("/statemachine/1.0.0") // protocol ID for the test

type FindNodeRequest[K kad.Key[K], A kad.Address[A]] struct {
	NodeID kad.NodeID[K]
}

func (r FindNodeRequest[K, A]) Target() K {
	return r.NodeID.Key()
}

func (FindNodeRequest[K, A]) EmptyResponse() kad.Response[K, A] {
	return &FindNodeResponse[K, A]{}
}

type FindNodeResponse[K kad.Key[K], A kad.Address[A]] struct {
	NodeID      kad.NodeID[K] // node we were looking for
	CloserPeers []kad.NodeInfo[K, A]
}

func (r *FindNodeResponse[K, A]) CloserNodes() []kad.NodeInfo[K, A] {
	return r.CloserPeers
}
