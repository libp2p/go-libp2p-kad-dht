package reprovider

import (
	"context"
	"testing"

	ds "github.com/ipfs/go-datastore"
	mh "github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
)

func TestMHStoreStoreAndGet(t *testing.T) {
	store, err := NewMHStore(context.Background(), ds.NewMapDatastore())
	if err != nil {
		t.Fatal(err)
	}

	mhs := make([]mh.Multihash, 6)
	for i := range mhs {
		h, err := mh.Sum([]byte{byte(i)}, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		mhs[i] = h
	}

	added, err := store.Put(context.Background(), mhs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != len(mhs) {
		t.Fatalf("expected %d new hashes, got %d", len(mhs), len(added))
	}

	added, err = store.Put(context.Background(), mhs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 0 {
		t.Fatalf("expected no new hashes on second put, got %d", len(added))
	}

	for _, h := range mhs {
		prefix := bitstr.Key(key.BitString(mhToBit256(h))[:DefaultMHStorePrefixLen])
		got, err := store.Get(context.Background(), prefix)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, m := range got {
			if string(m) == string(h) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected to find multihash %v for prefix %s", h, prefix)
		}
	}

	short := DefaultMHStorePrefixLen / 2
	p := bitstr.Key(key.BitString(mhToBit256(mhs[0]))[:short])
	res, err := store.Get(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatalf("expected results for prefix %s", p)
	}

	longPrefix := bitstr.Key(key.BitString(mhToBit256(mhs[0]))[:15])
	res, err = store.Get(context.Background(), longPrefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range res {
		bs := bitstr.Key(key.BitString(mhToBit256(h)))
		if bs[:15] != longPrefix {
			t.Fatalf("returned hash does not match long prefix")
		}
	}
}

func TestMHStoreReset(t *testing.T) {
	store, err := NewMHStore(context.Background(), ds.NewMapDatastore())
	if err != nil {
		t.Fatal(err)
	}

	first := make([]mh.Multihash, 2)
	for i := range first {
		h, err := mh.Sum([]byte{byte(i)}, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		first[i] = h
	}
	if _, err := store.Put(context.Background(), first...); err != nil {
		t.Fatal(err)
	}

	second := make([]mh.Multihash, 2)
	for i := range second {
		h, err := mh.Sum([]byte{byte(i + 10)}, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		second[i] = h
	}

	added, err := store.Reset(context.Background(), second...)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != len(second) {
		t.Fatalf("expected %d hashes after reset, got %d", len(second), len(added))
	}

	// old hashes should not be present
	for _, h := range first {
		prefix := bitstr.Key(key.BitString(mhToBit256(h))[:DefaultMHStorePrefixLen])
		got, err := store.Get(context.Background(), prefix)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range got {
			if string(m) == string(h) {
				t.Fatalf("expected old hash %v to be removed", h)
			}
		}
	}

	// new hashes should be retrievable
	for _, h := range second {
		prefix := bitstr.Key(key.BitString(mhToBit256(h))[:DefaultMHStorePrefixLen])
		got, err := store.Get(context.Background(), prefix)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, m := range got {
			if string(m) == string(h) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected hash %v after reset", h)
		}
	}
}

func TestMHStoreDelete(t *testing.T) {
	store, err := NewMHStore(context.Background(), ds.NewMapDatastore())
	if err != nil {
		t.Fatal(err)
	}

	mhs := make([]mh.Multihash, 3)
	for i := range mhs {
		h, err := mh.Sum([]byte{byte(i)}, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		mhs[i] = h
	}
	if _, err := store.Put(context.Background(), mhs...); err != nil {
		t.Fatal(err)
	}

	delPrefix := bitstr.Key(key.BitString(mhToBit256(mhs[0]))[:DefaultMHStorePrefixLen])
	if err := store.Delete(context.Background(), mhs[0]); err != nil {
		t.Fatal(err)
	}

	res, err := store.Get(context.Background(), delPrefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range res {
		if string(h) == string(mhs[0]) {
			t.Fatalf("expected no hashes for prefix after delete")
		}
	}

	// other hashes should still be retrievable
	otherPrefix := bitstr.Key(key.BitString(mhToBit256(mhs[1]))[:DefaultMHStorePrefixLen])
	res, err = store.Get(context.Background(), otherPrefix)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatalf("expected remaining hashes for other prefix")
	}
}
