export IPFS_API ?= v04x.ipfs.io

deps:
	env GO111MODULE=on go get ./...
