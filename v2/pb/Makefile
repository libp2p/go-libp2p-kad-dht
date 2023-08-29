all: clone reset
	protoc --go_out=./ --go_opt=Mgithub.com/libp2p/go-libp2p-record/pb/record.proto=github.com/libp2p/go-libp2p-record/pb ./dht.proto

clone:
	git clone --depth=1 git@github.com:libp2p/go-libp2p-record.git github.com/libp2p/go-libp2p-record || true

reset:
	rm dht.pb.go

clean:
	rm -rf github.com
