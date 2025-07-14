# Provider Interface

## Suggested `DHTProvider` interface

```go
// DHTProvider is an interface for providing keys to a DHT swarm. It holds a
// state of keys to be advertised, and is responsible for periodically
// publishing provider records for these keys to the DHT swarm before the
// records expire.
type DHTProvider interface {
  // StartProviding ensures keys are periodically advertised to the DHT swarm.
  //
  // If the keys aren't currently being reprovided, they are immediately
  // provided to the DHT swarm, and scheduled to be reprovided periodically. If
  // `force` is set to true, all keys are immediately provided to the DHT
  // swarm, regardless of whether they were already being provided in the past.
  // `keys` keep being reprovided until `StopProviding` is called.
  //
  // This call blocks until the initial provide completes (or fails), at
  // which point it returns any error related to the initial provide.
  //
  // Cancelling the context cancels only the immediate provide, but not the
  // reproviding of keys which is still done at some point the future. Call
  // `StopProviding` to stop reproviding the specified keys.
  StartProviding(ctx context.Context, force bool, keys ...mh.Multihash) error

  // StartProvidingAsync is similar to `StartProviding`, but it does not block
  // until the initial provide operation completes or fails. The supplied keys
  // will eventually be provided and reprovided to the DHT swarm until
  // `StopProviding` is called.
  StartProvidingAsync(force bool, keys ...mh.Multihash)

  // StopProviding stops reproviding the given keys to the DHT swarm. The node
  // stops being referred as a provider when the provider records in the DHT
  // swarm expire.
  StopProviding(...mh.Multihash)

  // ProvideOnce sends provider records for the specified keys to the DHT swarm
  // only once. It does not automatically reprovide those keys afterward.
  //
  // Canceling the context cancels the immediate provide of the keys.
  ProvideOnce(context.Context, ...mh.Multihash) error
}
```

## Old `boxo/provider` interface

```go
// Provider announces blocks to the network
type Provider interface {
  // Provide takes a cid and makes an attempt to announce it to the network
  Provide(context.Context, cid.Cid, bool) error
}

// Reprovider reannounces blocks to the network
type Reprovider interface {
  // Reprovide starts a new reprovide if one isn't running already.
  Reprovide(context.Context) error
}

// System defines the interface for interacting with the value
// provider system
type System interface {
  Close() error
  Stat() (ReproviderStats, error)
  Provider
  Reprovider
}
```

[source](https://github.com/ipfs/boxo/blob/44137d7d622ade477c6efc190ec2d5414fa3fcf8/provider/provider.go#L22-L41)
