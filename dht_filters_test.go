package dht

import (
	"testing"

	"github.com/multiformats/go-multiaddr"
)

func TestIsRelay(t *testing.T) {
	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/5002/p2p/QmdPU7PfRyKehdrP5A3WqmjyD6bhVpU1mLGKppa2FjGDjZ/p2p-circuit/p2p/QmVT6GYwjeeAF5TR485Yc58S3xRF5EFsZ5YAF4VcP3URHt")
	if !isRelayAddr(a) {
		t.Fatalf("thought %s was not a relay", a)
	}
	a, _ = multiaddr.NewMultiaddr("/p2p-circuit/p2p/QmVT6GYwjeeAF5TR485Yc58S3xRF5EFsZ5YAF4VcP3URHt")
	if !isRelayAddr(a) {
		t.Fatalf("thought %s was not a relay", a)
	}
	a, _ = multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/5002/p2p/QmdPU7PfRyKehdrP5A3WqmjyD6bhVpU1mLGKppa2FjGDjZ")
	if isRelayAddr(a) {
		t.Fatalf("thought %s was a relay", a)
	}

}
