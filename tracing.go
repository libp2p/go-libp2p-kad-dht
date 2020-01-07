package dht

import (
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

// LinkedFinish finishes this Span, marking it as errored and recording a
// log if *err is not nil. It uses a pointer so it's defer-friendly.
func LinkedFinish(sp opentracing.Span, err *error) {
	if err != nil && *err != nil {
		sp.SetTag("error", true)
		sp.LogFields(log.Error(*err))
	}
	sp.Finish()
}

func loggableKey(k string) string {
	if len(k) == 0 {
		return k
	}

	var ns, key string
	switch k[0] {
	case '/':
		// it's a namespaced path (probably)
		protoEnd := strings.IndexByte(k[1:], '/')
		if protoEnd < 0 {
			return k
		}
		ns = k[1 : protoEnd+1]
		key = k[protoEnd+2:]
	default:
		ns = "provider"
		key = k
	}

	c, err := cid.Cast([]byte(key))
	if err != nil {
		return k
	}
	return fmt.Sprintf("/%s/%s", ns, c.String())
}
