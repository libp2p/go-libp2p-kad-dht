all: clone reset build clean

build:
	protoc --go_out=./ --go_opt=Mgithub.com/libp2p/go-libp2p-record/pb/record.proto=github.com/libp2p/go-libp2p-record/pb ./dht.proto

clone:
	git -c advice.detachedHead=false clone --depth 1 --branch v0.2.0 https://github.com/libp2p/go-libp2p-record.git github.com/libp2p/go-libp2p-record || true

reset:
	rm dht.pb.go || true

clean:
	rm -rf github.com

.PHONY: all build clone reset clean