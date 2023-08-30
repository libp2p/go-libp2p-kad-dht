# Protocol Buffers

To generate the protobuf definitions run:

```shell
make
```

This command will clone the `libp2p/go-libp2p-record` repository into this
directory (git-ignored) and run the `protoc` command to generate the `dht.pb.go` file for the
`dht.proto` protobuf definition. We need `go-libp2p-record` because `dht.proto`
reference the `Record` protobuf definition from that repository.

To clean up after you have generated the `dht.pb.go` file, you can run:

```shell
make clean
```