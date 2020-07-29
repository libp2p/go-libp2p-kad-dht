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
	if k != "/proto/"+multibaseB32Encode(c.Bytes()) {
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
	b32MH := multibaseB32Encode(c0.Hash())
	k, err := tryFormatLoggableProviderKey(c0.Bytes())
	if err != nil {
		t.Errorf("failed to format key: %s", err)
	}
	if k != b32MH {
		t.Error("expected cidv0 to be converted into base32 multihash")
	}

	// Test logging CIDv1 provider (from older DHT implementations)
	c1 := cid.NewCidV1(cid.DagProtobuf, c0.Hash())
	k, err = tryFormatLoggableProviderKey(c1.Hash())
	if err != nil {
		t.Errorf("failed to format key: %s", err)
	}
	if k != b32MH {
		t.Error("expected cidv1 to be converted into base32 multihash")
	}

	// Test logging multihash provider
	k, err = tryFormatLoggableProviderKey(c1.Hash())
	if err != nil {
		t.Errorf("failed to format key: %s", err)
	}
	if k != b32MH {
		t.Error("expected multihash to be displayed in base32")
	}

	for _, s := range []string{"/bla", "", "bla bla", "/bla/asdf", "/a/b/c"} {
		if _, err := tryFormatLoggableProviderKey([]byte(s)); err == nil {
			t.Errorf("expected to fail formatting: %s", s)
		}
	}
}
