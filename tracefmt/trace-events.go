package datafmts

import (
  "time"

  peer "github.com/libp2p/go-libp2p-core/peer"
  pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

// state of a rate limiter thing (like a channel)
type RateLimitState struct {
  Capacity int
  Length   int
}

// PUT_VALUE, GET_VALUE, ADD_PROVIDER, GET_PROVIDERS, FIND_NODE, PING
// https://github.com/libp2p/go-libp2p-kad-dht/blob/master/pb/dht.proto#L14-L21
type QueryType string

type Query struct {
  Key         string
  Type        QueryType
  Concurrency int
}

// the current state of this object:
// https://github.com/libp2p/go-libp2p-kad-dht/blob/master/query.go#L81-L98
type QueryRunnerState struct {
  Query           Query
  PeersSeen       []peer.ID
  PeersQueried    []peer.ID
  PeersDialed     []peer.ID
  PeersDialedNew  []peer.ID
  PeersDialQueueLen int
  PeersToQueryLen int
  PeersRemainingLen int32
  Result          QueryResult
  RateLimit       RateLimitState
  StartTime       time.Time
  CurrTime        time.Time
  EndTime         time.Time
}

type QueryResult struct {
  Success     bool
  FoundPeer   peer.ID
  CloserPeers []peer.ID
  FinalSet    []peer.ID
  QueriedSet  []peer.ID
}

// SpawnWorkerEvent tracks event loop in this func
// https://github.com/libp2p/go-libp2p-kad-dht/blob/master/query.go#L208-L237
type SpawnWorkerEvent struct {
  Runner QueryRunnerState
}

var (
  DialStart       = QueryPeerEventType("DialStart")
  DialFailed      = QueryPeerEventType("DialFailed")
  Connected       = QueryPeerEventType("Connected")
  DHTSentRequest  = QueryPeerEventType("DHTSentRequest")
  DHTRecvResponse = QueryPeerEventType("DHTRecvReply")
)

type QueryPeerEventType string

// signals starting to dial a party
type QueryPeerEvent struct {
  Type QueryPeerEventType // string
  Peer peer.ID
  Time time.Time

  Request  DHTMessage // if any
  Response DHTMessage // if any
}

// DHT message contents
type DHTMessage struct {
  Type          pb.Message_MessageType
  Key           string
  Record        pb.Record // if any
  CloserPeers   []peer.ID // if any
  ProviderPeers []peer.ID // if any

  // protobuf contents
  // https://github.com/libp2p/go-libp2p-kad-dht/blob/master/pb/dht.proto#L13-L71
  DHTMessage pb.Message
}
