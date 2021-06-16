package dht

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-routing-language/syntax"
)

var in = syntax.Dict{syntax.Pairs{syntax.Pair{
	Key:   syntax.String{"hello"},
	Value: syntax.String{"world"},
}}}

func TestValueSmartGetSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var dhts [5]*IpfsDHT

	for i := range dhts {
		dhts[i] = setupDHT(ctx, t, false, EnableSmartRecords())
		defer dhts[i].Close()
		defer dhts[i].host.Close()
	}

	connect(t, ctx, dhts[0], dhts[1])

	t.Log("adding value on: ", dhts[0].self)
	ctxT, cancel := context.WithTimeout(ctx, 2000*time.Second)
	defer cancel()
	err := dhts[0].PutSmartValue(ctxT, "/v/hello", in)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("requesting value on dhts: ", dhts[1].self)
	ctxT, cancel = context.WithTimeout(ctx, 2000*time.Second)
	defer cancel()

	val, err := dhts[1].GetSmartValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}
	if len(val) == 0 {
		t.Fatal("wrong size in returned value")
	}
	out := *val[dhts[0].self]
	if !syntax.IsEqual(out, in) {
		t.Fatalf("Expected %v but got %v", in, out)
	}

	// late connect

	connect(t, ctx, dhts[2], dhts[0])
	connect(t, ctx, dhts[2], dhts[1])

	t.Log("requesting value (offline) on dhts: ", dhts[2].self)
	vala, err := dhts[2].GetSmartValue(ctxT, "/v/hello", Quorum(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(vala) == 0 {
		t.Fatal("wrong size in returned value")
	}
	out = *vala[dhts[0].self]
	if !syntax.IsEqual(out, in) {
		t.Fatalf("Expected %v but got %v", in, out)
	}

	t.Log("requesting value (online) on dhts: ", dhts[2].self)
	val, err = dhts[2].GetSmartValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}
	if len(val) == 0 {
		t.Fatal("wrong size in returned value")
	}
	out = *val[dhts[0].self]
	if !syntax.IsEqual(out, in) {
		t.Fatalf("Expected %v but got %v", in, out)
	}

	for _, d := range dhts[:3] {
		connect(t, ctx, dhts[3], d)
	}
	connect(t, ctx, dhts[4], dhts[3])

	// TODO: This test is not passing. It is not able to find the smart-record.
	// It only looks in one peer (the one it is connected to).
	// I am probably missing something but have no time to debug.
	t.Log("requesting value (requires peer routing) on dhts: ", dhts[4].self)
	val, err = dhts[4].GetSmartValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}
	if len(val) == 0 {
		t.Fatal("wrong size in returned value when peer routing required")
	}
	out = *val[dhts[0].self]
	if !syntax.IsEqual(out, in) {
		t.Fatalf("Expected %v but got %v", in, out)
	}
}
