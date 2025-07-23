package provider

import (
	"time"

	mh "github.com/multiformats/go-multihash"
)

// DHTProvider is an interface for providing keys to a DHT swarm. It holds a
// state of keys to be advertised, and is responsible for periodically
// publishing provider records for these keys to the DHT swarm before the
// records expire.
type DHTProvider interface {
	// StartProviding ensures keys are periodically advertised to the DHT swarm.
	//
	// If the `keys` aren't currently being reprovided, they are added to the
	// queue to be provided to the DHT swarm as soon as possible, and scheduled
	// to be reprovided periodically. If `force` is set to true, all keys are
	// provided to the DHT swarm, regardless of whether they were already being
	// reprovided in the past. `keys` keep being reprovided until `StopProviding`
	// is called.
	//
	// This operation is asynchronous, it returns as soon as the `keys` are added
	// to the provide queue, and provides happens asynchronously.
	StartProviding(force bool, keys ...mh.Multihash)

	// StopProviding stops reproviding the given keys to the DHT swarm. The node
	// stops being referred as a provider when the provider records in the DHT
	// swarm expire.
	//
	// Remove the `keys` from the schedule and return immediately. Valid records
	// can remain in the DHT swarm up to the provider record TTL after calling
	// `StopProviding`.
	StopProviding(keys ...mh.Multihash)

	// ProvideOnce sends provider records for the specified keys to the DHT swarm
	// only once. It does not automatically reprovide those keys afterward.
	//
	// Add the supplied multihashes to the provide queue, and return immediately.
	// The provide operation happens asynchronously.
	ProvideOnce(keys ...mh.Multihash)
}

var _ DHTProvider = &SweepingProvider{}

type SweepingProvider struct {
	// TODO: implement me
}

// ProvideOnce sends provider records for the specified keys to the DHT swarm
// only once. It does not automatically reprovide those keys afterward.
//
// Add the supplied multihashes to the provide queue, and return immediately.
// The provide operation happens asynchronously.
func (s *SweepingProvider) ProvideOnce(keys ...mh.Multihash) {
	// TODO: implement me
}

// StartProviding ensures keys are periodically advertised to the DHT swarm.
//
// If the `keys` aren't currently being reprovided, they are added to the
// queue to be provided to the DHT swarm as soon as possible, and scheduled
// to be reprovided periodically. If `force` is set to true, all keys are
// provided to the DHT swarm, regardless of whether they were already being
// reprovided in the past. `keys` keep being reprovided until `StopProviding`
// is called.
//
// This operation is asynchronous, it returns as soon as the `keys` are added
// to the provide queue, and provides happens asynchronously.
func (s *SweepingProvider) StartProviding(force bool, keys ...mh.Multihash) {
	// TODO: implement me
}

// StopProviding stops reproviding the given keys to the DHT swarm. The node
// stops being referred as a provider when the provider records in the DHT
// swarm expire.
//
// Remove the `keys` from the schedule and return immediately. Valid records
// can remain in the DHT swarm up to the provider record TTL after calling
// `StopProviding`.
func (s *SweepingProvider) StopProviding(keys ...mh.Multihash) {
	// TODO: implement me
}

// ProvideState encodes the current relationship between this node and `key`.
type ProvideState uint8

const (
	StateUnknown  ProvideState = iota // we have no record of the key
	StateQueued                       // key is queued to be provided
	StateProvided                     // key was provided at least once
)

// ProvideStatus reports the provider’s view of a key.
//
// When `state == StateProvided`, `lastProvide` is the wall‑clock time of the
// most recent successful provide operation (UTC).
// For `StateQueued` or `StateUnknown`, `lastProvide` is the zero `time.Time`.
func (s *SweepingProvider) ProvideStatus(key mh.Multihash) (state ProvideState, lastProvide time.Time) {
	return StateUnknown, time.Time{}
}
