package dht

import (
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-base32"
	"github.com/multiformats/go-multihash"
)

func lowercaseB32Encode(k []byte) string {
	return strings.ToLower(base32.RawStdEncoding.EncodeToString(k))
}

func tryFormatLoggableRecordKey(k string) (string, error) {
	if len(k) == 0 {
		return "", fmt.Errorf("loggableRecordKey is empty")
	}
	var proto, cstr string
	if k[0] == '/' {
		// it's a path (probably)
		protoEnd := strings.IndexByte(k[1:], '/')
		if protoEnd < 0 {
			return "", fmt.Errorf("loggableRecordKey starts with '/' but is not a path: %s", lowercaseB32Encode([]byte(k)))
		}
		proto = k[1 : protoEnd+1]
		cstr = k[protoEnd+2:]

		encStr := lowercaseB32Encode([]byte(cstr))
		return fmt.Sprintf("/%s/%s", proto, encStr), nil
	}

	return "", fmt.Errorf("loggableRecordKey is not a path: %s", lowercaseB32Encode([]byte(cstr)))
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
	newKey, err := tryFormatLoggableProviderKey(lk)
	if err == nil {
		return newKey
	}
	return err.Error()
}

func tryFormatLoggableProviderKey(k []byte) (string, error) {
	if len(k) == 0 {
		return "", fmt.Errorf("loggableProviderKey is empty")
	}

	encodedKey := lowercaseB32Encode(k)

	// The DHT used to provide CIDs, but now provides multihashes
	// TODO: Drop this when enough of the network has upgraded
	if _, err := cid.Cast(k); err == nil {
		return encodedKey, nil
	}

	if _, err := multihash.Cast(k); err == nil {
		return encodedKey, nil
	}

	return "", fmt.Errorf("loggableProviderKey is not a Multihash or CID: %s", encodedKey)
}
