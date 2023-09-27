package tele

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"go.uber.org/zap/exp/zapslog"
	"golang.org/x/exp/slog"
)

func DefaultLogger(name string) *slog.Logger {
	return slog.New(zapslog.NewHandler(logging.Logger(name).Desugar().Core()))
}

// Attributes that can be used with logging or tracing
const (
	AttrKeyError    = "error"
	AttrKeyPeerID   = "peer_id"
	AttrKeyKey      = "key"
	AttrKeyCacheHit = "hit"
	AttrKeyInEvent  = "in_event"
	AttrKeyOutEvent = "out_event"
)

func LogAttrError(err error) slog.Attr {
	return slog.Attr{Key: AttrKeyError, Value: slog.AnyValue(err)}
}

func LogAttrPeerID(id kadt.PeerID) slog.Attr {
	return slog.String(AttrKeyPeerID, id.String())
}

func LogAttrKey(kk kadt.Key) slog.Attr {
	return slog.String(AttrKeyKey, kk.HexString())
}
