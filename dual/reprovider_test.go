package dual

import (
	"context"
	"strconv"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func genCids(n int) []cid.Cid {
	cids := make([]cid.Cid, n)
	for i := range n {
		h, err := mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		c := cid.NewCidV1(cid.Raw, h)
		cids[i] = c
	}
	return cids
}

func TestResetReprovideSet(t *testing.T) {
	ctx := context.Background()
	cids := genCids(1024)
	mhChan := make(chan mh.Multihash, 8)
	go func() {
		defer close(mhChan)
		for _, c := range cids {
			mhChan <- c.Hash()
		}
	}()

	h, err := libp2p.New()
	require.NoError(t, err)

	dht, err := New(ctx, h)
	require.NoError(t, err)

	r, err := dht.NewSweepingReprovider()
	require.NoError(t, err)

	err = r.ResetReprovideSet(ctx, mhChan)
	require.NoError(t, err)
}
