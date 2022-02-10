package mqpeerset

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/assert"
)

func TestNewMultiQueryPeerset(t *testing.T) {
	key := "test"
	mqps := NewMultiQueryPeerset(key)
	assert.True(t, bytes.Equal(mqps.key.Original, []byte(key)))
	assert.NotNil(t, mqps.states)
	assert.Len(t, mqps.states, 0)
	assert.NotNil(t, mqps.intersections)
	assert.Len(t, mqps.intersections, 0)

	queryID1 := uuid.New()
	queryID2 := uuid.New()
	mqps = NewMultiQueryPeerset(key, queryID1, queryID2)
	assert.Len(t, mqps.states, 2)
	assert.NotNil(t, mqps.states[queryID1])
	assert.NotNil(t, mqps.states[queryID2])
	assert.NotNil(t, mqps.intersections)
	assert.Len(t, mqps.intersections, 0)
}

func TestMultiQueryPeerset_TryAdd_panicsForUnknownQuery(t *testing.T) {
	key := "test"
	queryID1 := uuid.New()
	queryID2 := uuid.New()
	queryID3 := uuid.New()

	peer1 := test.RandPeerIDFatal(t)
	peer2 := test.RandPeerIDFatal(t)

	mqps := NewMultiQueryPeerset(key, queryID1, queryID2)

	assert.Panics(t, func() {
		mqps.TryAdd(queryID3, peer1, peer2)
	})
}

func TestMultiQueryPeerset_TryAdd_addsCorrectMultiQueryPeerState(t *testing.T) {
	key := "test"
	queryID1 := uuid.New()
	queryID2 := uuid.New()

	peer1 := test.RandPeerIDFatal(t)
	peer2 := test.RandPeerIDFatal(t)
	peer3 := test.RandPeerIDFatal(t)

	mqps := NewMultiQueryPeerset(key, queryID1, queryID2)

	assert.True(t, mqps.TryAdd(queryID2, peer1, peer2))
	assert.Equal(t, mqps.states[queryID2][peer1].id, peer1)
	assert.NotNil(t, mqps.states[queryID2][peer1].distance)
	assert.Equal(t, mqps.states[queryID2][peer1].state, PeerHeard)
	assert.Equal(t, mqps.states[queryID2][peer1].referredBy, peer2)

	assert.False(t, mqps.TryAdd(queryID2, peer1, peer3))
	assert.Equal(t, mqps.states[queryID2][peer1].referredBy, peer2)
}

func TestMultiQueryPeerset_TryAdd_updatesIntersections(t *testing.T) {
	key := "test"
	queryID1 := uuid.New()
	queryID2 := uuid.New()

	peer1 := test.RandPeerIDFatal(t)
	peer2 := test.RandPeerIDFatal(t)
	peer3 := test.RandPeerIDFatal(t)

	mqps := NewMultiQueryPeerset(key, queryID1, queryID2)

	assert.Len(t, mqps.intersections, 0)

	assert.True(t, mqps.TryAdd(queryID1, peer1, peer2))
	mqps.SetState(queryID1, peer1, PeerWaiting)
	assert.True(t, mqps.TryAdd(queryID2, peer1, peer3))

	assert.Len(t, mqps.intersections, 1)
	assert.NotNil(t, mqps.intersections[peer1])
	assert.Equal(t, peer1, mqps.intersections[peer1].ID)
	assert.Equal(t, PeerWaiting, mqps.intersections[peer1].State)
	assert.NotNil(t, mqps.intersections[peer1].Distance)

	mqps.SetState(queryID1, peer1, PeerQueried)
	assert.Equal(t, PeerQueried, mqps.intersections[peer1].State)
}
