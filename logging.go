package dht

import (
	"fmt"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-base32"
)

func tryFormatLoggableRecordKey(k string) (string, error) {
	if len(k) == 0 {
		return "", fmt.Errorf("loggableRecordKey is empty")
	}
	var proto, cstr string
	if k[0] == '/' {
		// it's a path (probably)
		protoEnd := strings.IndexByte(k[1:], '/')
		if protoEnd < 0 {
			return "", fmt.Errorf("loggableRecordKey starts with '/' but is not a path: %s", base32.RawStdEncoding.EncodeToString([]byte(k)))
		}
		proto = k[1 : protoEnd+1]
		cstr = k[protoEnd+2:]

		var encStr string
		c, err := cid.Cast([]byte(cstr))
		if err == nil {
			encStr = c.String()
		} else {
			encStr = base32.RawStdEncoding.EncodeToString([]byte(cstr))
		}

		return fmt.Sprintf("/%s/%s", proto, encStr), nil
	}

	return "", fmt.Errorf("loggableRecordKey is not a path: %s", base32.RawStdEncoding.EncodeToString([]byte(k)))
}

type loggableRecordKeyString string

func (lk loggableRecordKeyString) String() string {
	k := string(lk)
	newKey, err := tryFormatLoggableRecordKey(k)
	if err == nil {
		return newKey
	}
	return err.Error()
}

type loggableRecordKeyBytes []byte

func (lk loggableRecordKeyBytes) String() string {
	k := string(lk)
	newKey, err := tryFormatLoggableRecordKey(k)
	if err == nil {
		return newKey
	}
	return err.Error()
}

type loggableProviderRecordBytes []byte

func (lk loggableProviderRecordBytes) String() string {
	k := string(lk)
	newKey, err := tryFormatLoggableProviderKey(k)
	if err == nil {
		return newKey
	}
	return err.Error()
}

func tryFormatLoggableProviderKey(k string) (string, error) {
	if len(k) == 0 {
		return "", fmt.Errorf("loggableProviderKey is empty")
	}

	h, err := multihash.Cast([]byte(k))
	if err == nil {
		c := cid.NewCidV1(cid.Raw, h)
		encStr, err := c.StringOfBase(multibase.Base32)
		if err != nil {
			panic(fmt.Errorf("should be impossible to reach here : %w", err))
		}
		return encStr, nil
	}

	// The DHT used to provide CIDs, but now provides multihashes
	// TODO: Drop this when enough of the network has upgraded
	c, err := cid.Cast([]byte(k))
	if err == nil {
		encStr, err := c.StringOfBase(multibase.Base32)
		if err != nil {
			panic(fmt.Errorf("should be impossible to reach here : %w", err))
		}
		return encStr, nil
	}

	return "", fmt.Errorf("invalid provider record: %s : err %w", base32.RawStdEncoding.EncodeToString([]byte(k)), err)
}
