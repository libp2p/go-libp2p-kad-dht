package dht

import (
	"testing"

	cid "github.com/ipfs/go-cid"
)

func TestLoggableRecordKey(t *testing.T) {
	c, err := cid.Decode("QmfUvYQhL2GinafMbPDYz7VFoZv4iiuLuR33aRsPurXGag")
	if err != nil {
		t.Fatal(err)
	}

	k, err := tryFormatLoggableRecordKey("/proto/" + string(c.Bytes()))
	if err != nil {
		t.Errorf("failed to format key: %s", err)
	}
	if k != "/proto/"+c.String() {
		t.Error("expected path to be preserved as a loggable key")
	}

	for _, s := range []string{"/bla", "", "bla bla"} {
		if _, err := tryFormatLoggableRecordKey(s); err == nil {
			t.Errorf("expected to fail formatting: %s", s)
		}
	}

	for _, s := range []string{"/bla/asdf", "/a/b/c"} {
		if _, err := tryFormatLoggableRecordKey(s); err != nil {
			t.Errorf("expected to be formatable: %s", s)
		}
	}
}

func TestLoggableProviderKey(t *testing.T) {
	c0, err := cid.Decode("QmfUvYQhL2GinafMbPDYz7VFoZv4iiuLuR33aRsPurXGag")
	if err != nil {
		t.Fatal(err)
	}

	// Test logging CIDv0 provider
	c0ascidv1Raw := cid.NewCidV1(cid.Raw, c0.Hash())
	k, err := tryFormatLoggableProviderKey(string(c0.Bytes()))
	if err != nil {
		t.Errorf("failed to format key: %s", err)
	}
	if k != c0ascidv1Raw.String() {
		t.Error("expected cidv0 to be converted into CIDv1 b32 with Raw codec")
	}

	// Test logging CIDv1 provider (from older DHT implementations)
	c1 := cid.NewCidV1(cid.DagProtobuf, c0.Hash())
	k, err = tryFormatLoggableProviderKey(string(c1.Bytes()))
	if err != nil {
		t.Errorf("failed to format key: %s", err)
	}
	if k != c1.String() {
		t.Error("expected cidv1 to be displayed normally")
	}

	// Test logging multihash provider
	c1ascidv1Raw := cid.NewCidV1(cid.Raw, c1.Hash())
	k, err = tryFormatLoggableProviderKey(string(c1.Hash()))
	if err != nil {
		t.Errorf("failed to format key: %s", err)
	}
	if k != c1ascidv1Raw.String() {
		t.Error("expected multihash to be converted into CIDv1 b32 with Raw codec")
	}

	for _, s := range []string{"/bla", "", "bla bla", "/bla/asdf", "/a/b/c"} {
		if _, err := tryFormatLoggableProviderKey(s); err == nil {
			t.Errorf("expected to fail formatting: %s", s)
		}
	}
}
