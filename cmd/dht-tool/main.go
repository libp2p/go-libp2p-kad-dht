package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	multiaddr "github.com/multiformats/go-multiaddr"

	host "github.com/libp2p/go-libp2p-host"
	pstore "github.com/libp2p/go-libp2p-peerstore"

	"github.com/anacrolix/ipfslog"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
)

func main() {
	err := errMain()
	if err != nil {
		log.Fatal(err)
	}
}

func errMain() error {
	ipfslog.SetAllLoggerLevels(ipfslog.Warning)
	ipfslog.SetModuleLevel("dht", ipfslog.Info)
	host, err := libp2p.New(context.Background())
	if err != nil {
		return fmt.Errorf("error creating host: %s", err)
	}
	defer host.Close()
	d, err := dht.New(context.Background(), host)
	if err != nil {
		return fmt.Errorf("error creating dht node: %s", err)
	}
	defer d.Close()
	bootstrapNodeAddrs := dht.DefaultBootstrapPeers
	numConnected := connectToBootstrapNodes(host, bootstrapNodeAddrs)
	if numConnected == 0 {
		return fmt.Errorf("failed to connect to any bootstrap nodes: %s", err)
	} else {
		log.Printf("connected to %d/%d bootstrap nodes", numConnected, len(bootstrapNodeAddrs))
	}
	err = d.Bootstrap(context.Background())
	if err != nil {
		return fmt.Errorf("error bootstrapping dht: %s", err)
	}
	select {}
}

func connectToBootstrapNodes(h host.Host, mas []multiaddr.Multiaddr) (numConnected int32) {
	var wg sync.WaitGroup
	for _, ma := range mas {
		wg.Add(1)
		go func(ma multiaddr.Multiaddr) {
			pi, err := pstore.InfoFromP2pAddr(ma)
			if err != nil {
				panic(err)
			}
			defer wg.Done()
			err = h.Connect(context.Background(), *pi)
			if err != nil {
				log.Printf("error connecting to bootstrap node %q: %v", ma, err)
			} else {
				atomic.AddInt32(&numConnected, 1)
			}
		}(ma)
	}
	wg.Wait()
	return
}
