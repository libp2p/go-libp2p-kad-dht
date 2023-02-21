package netsize

import (
	"testing"
	"time"

	kbucket "github.com/libp2p/go-libp2p-kbucket"
	pt "github.com/libp2p/go-libp2p/core/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ks "github.com/whyrusleeping/go-keyspace"
)

func TestNewEstimator(t *testing.T) {
	bucketSize := 20

	pid, err := pt.RandPeerID()
	require.NoError(t, err)

	rt, err := kbucket.NewRoutingTable(bucketSize, kbucket.ConvertPeerID(pid), time.Second, nil, time.Second, nil)
	require.NoError(t, err)

	e := NewEstimator(pid, rt, bucketSize)

	assert.Equal(t, rt, e.rt)
	assert.Equal(t, kbucket.ConvertPeerID(pid), e.localID)
	assert.Len(t, e.measurements, bucketSize)
	assert.Equal(t, invalidEstimate, e.netSizeCache)
}

func TestNormedDistance(t *testing.T) {
	pid, err := pt.RandPeerID()
	require.NoError(t, err)

	dist := NormedDistance(pid, ks.XORKeySpace.Key([]byte(pid)))
	assert.Zero(t, dist)

	pid2, err := pt.RandPeerID()
	require.NoError(t, err)

	dist = NormedDistance(pid, ks.XORKeySpace.Key([]byte(pid2)))
	assert.Greater(t, 1.0, dist)
	assert.Less(t, dist, 1.0)
}
