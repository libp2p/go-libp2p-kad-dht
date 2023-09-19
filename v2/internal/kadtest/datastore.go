package kadtest

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

// HookedDatastore provides hook into HookedDatastore operations. Use [NewHookedDatastore] to
// initialize this HookedDatastore with no-op methods for each operation. Additional
// hooks can be added as needed. Only a subset is supported at the moment
//
// The idea is to wrap an ordinary [ds.Datastore] with this [HookedDatastore] and
// overwrite the Before/After hooks to, e.g., synchronize tests and assert
// that certain operations have been performed.
type HookedDatastore struct {
	dstore ds.Datastore

	GetBefore func(ctx context.Context, key ds.Key)
	GetAfter  func(ctx context.Context, key ds.Key, value []byte, err error)

	HasBefore func(ctx context.Context, key ds.Key)
	HasAfter  func(ctx context.Context, key ds.Key, exists bool, err error)

	PutBefore func(ctx context.Context, key ds.Key, value []byte)
	PutAfter  func(ctx context.Context, key ds.Key, value []byte, err error)

	DeleteBefore func(ctx context.Context, key ds.Key)
	DeleteAfter  func(ctx context.Context, key ds.Key, err error)
}

var _ ds.Datastore = (*HookedDatastore)(nil)

// NewHookedDatastore initializes a [HookedDatastore] with no-op hooks into calls to dstore.
// See [HookedDatastore] for more information.
func NewHookedDatastore(dstore ds.Datastore) *HookedDatastore {
	return &HookedDatastore{
		dstore:       dstore,
		GetBefore:    func(context.Context, ds.Key) {},
		GetAfter:     func(context.Context, ds.Key, []byte, error) {},
		HasBefore:    func(context.Context, ds.Key) {},
		HasAfter:     func(context.Context, ds.Key, bool, error) {},
		PutBefore:    func(context.Context, ds.Key, []byte) {},
		PutAfter:     func(context.Context, ds.Key, []byte, error) {},
		DeleteBefore: func(context.Context, ds.Key) {},
		DeleteAfter:  func(context.Context, ds.Key, error) {},
	}
}

func (d HookedDatastore) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	d.GetBefore(ctx, key)
	defer d.GetAfter(ctx, key, value, err)
	return d.dstore.Get(ctx, key)
}

func (d HookedDatastore) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	d.HasBefore(ctx, key)
	defer d.HasAfter(ctx, key, exists, err)
	return d.dstore.Has(ctx, key)
}

func (d HookedDatastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	return d.dstore.GetSize(ctx, key)
}

func (d HookedDatastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	return d.dstore.Query(ctx, q)
}

func (d HookedDatastore) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	d.PutBefore(ctx, key, value)
	defer d.PutAfter(ctx, key, value, err)
	return d.dstore.Put(ctx, key, value)
}

func (d HookedDatastore) Delete(ctx context.Context, key ds.Key) (err error) {
	d.DeleteBefore(ctx, key)
	defer d.DeleteAfter(ctx, key, err)
	return d.dstore.Delete(ctx, key)
}

func (d HookedDatastore) Sync(ctx context.Context, prefix ds.Key) error {
	return d.dstore.Sync(ctx, prefix)
}

func (d HookedDatastore) Close() error {
	return d.dstore.Close()
}
