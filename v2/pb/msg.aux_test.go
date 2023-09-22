package pb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessage_ExpectResponse(t *testing.T) {
	t.Run("all covered", func(t *testing.T) {
		defer func() {
			assert.Nil(t, recover())
		}()

		for msgTypeInt := range Message_MessageType_name {
			msg := &Message{Type: Message_MessageType(msgTypeInt)}
			msg.ExpectResponse()
		}
	})

	t.Run("unexpected type", func(t *testing.T) {
		defer func() {
			assert.NotNil(t, recover())
		}()
		msg := &Message{Type: Message_MessageType(-1)}
		msg.ExpectResponse()
	})
}

func TestMessage_Peer_invalid_maddr(t *testing.T) {
	msg := Message_Peer{
		Addrs: [][]byte{[]byte("invalid-maddr")},
	}

	if len(msg.Addresses()) > 0 {
		t.Fatal("shouldn't have any multiaddrs")
	}
}
