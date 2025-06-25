# Provider Interface

## Suggested `SweepingReprovider` interface

```go
type Provider interface {
  // StartProviding provides the given keys to the DHT swarm unless they were
  // already provided in the past. The keys will be periodically reprovided until
  // StopProviding is called for the same keys or user defined garbage collection
  // deletes the keys.
  StartProviding(...mh.Multihash)

  // StopProviding stops reproviding the given keys to the DHT swarm. The node
  // stops being referred as a provider when the provider records in the DHT
  // swarm expire.
  StopProviding(...mh.Multihash)

  // InstantProvide only sends provider records for the given keys out to the DHT
  // swarm. It does NOT take the responsibility to reprovide these keys.
  InstantProvide(context.Context, ...mh.Multihash) error

  // ForceProvide is similar to StartProviding, but it sends provider records out
  // to the DHT even if the keys were already provided in the past.
  ForceProvide(context.Context, ...mh.Multihash) error
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
