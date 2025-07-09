package provider

import (
	"context"

	mh "github.com/multiformats/go-multihash"
)

// Provider is an interface that defines the methods for DHT provides and
// reprovides.
//
// Note that this interface is subject to change.
type Provider interface {
	// ProvideOnce sends provider records for the specified keys to the DHT swarm
	// only once. It does not automatically reprovide those keys afterward.
	ProvideOnce(context.Context, ...mh.Multihash) error

	// StartProviding provides the given keys to the DHT swarm unless they were
	// already provided in the past. The keys will be periodically reprovided until
	// StopProviding is called for the same keys or user defined garbage collection
	// deletes the keys.
	StartProviding(...mh.Multihash)

	// ForceStartProviding is similar to StartProviding, but it sends provider
	// records out to the DHT regardless of whether the keys were already provided
	// in the past. It keeps reproviding the keys until StopProviding is called
	// for these keys.
	ForceStartProviding(context.Context, ...mh.Multihash) error

	// StopProviding stops reproviding the given keys to the DHT swarm. The node
	// stops being referred as a provider when the provider records in the DHT
	// swarm expire.
	StopProviding(...mh.Multihash)
}

var _ Provider = &SweepingProvider{}

type SweepingProvider struct {
	// TODO: implement me
}

// ProvideOnce only sends provider records for the given keys out to the DHT
// swarm. It does NOT take the responsibility to reprovide these keys.
func (s *SweepingProvider) ProvideOnce(ctx context.Context, keys ...mh.Multihash) error {
	// TODO: implement me
	return nil
}

// StartProviding provides the given keys to the DHT swarm unless they were
// already provided in the past. The keys will be periodically reprovided until
// StopProviding is called for the same keys or user defined garbage collection
// deletes the keys.
func (s *SweepingProvider) StartProviding(keys ...mh.Multihash) {
	// TODO: implement me
}

// ForceStartProviding is similar to StartProviding, but it sends provider
// records out to the DHT even if the keys were already provided in the past.
func (s *SweepingProvider) ForceStartProviding(ctx context.Context, keys ...mh.Multihash) error {
	// TODO: implement me
	return nil
}

// StopProviding stops reproviding the given keys to the DHT swarm. The node
// stops being referred as a provider when the provider records in the DHT
// swarm expire.
func (s *SweepingProvider) StopProviding(keys ...mh.Multihash) {
	// TODO: implement me
}
