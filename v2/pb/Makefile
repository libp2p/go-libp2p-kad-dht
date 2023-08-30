all: clone reset build clean

build:
	protoc --go_out=./ --go_opt=Mgithub.com/libp2p/go-libp2p-record/pb/record.proto=github.com/libp2p/go-libp2p-record/pb ./msg.proto

clone:
	git clone --depth 1 --branch v0.2.0 git@github.com:libp2p/go-libp2p-record.git github.com/libp2p/go-libp2p-record || true

reset:
	rm msg.pb.go || true

clean:
	rm -rf github.com

.PHONY: all build clone reset clean