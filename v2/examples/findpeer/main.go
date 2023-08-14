package main

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht/v2"
)

func main() {
	ctx := context.Background()

	h, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		panic(err)
	}

	d, err := dht.New(h, dht.DefaultConfig())
	if err != nil {
		panic(err)
	}

	go d.Start(ctx)

	addrInfo, err := d.FindPeer(ctx, "")
	if err != nil {
		panic(err)
	}
	fmt.Println(addrInfo)
}
