// +build !windows

package dht

import "syscall"

func curFileLimit() uint64 {
	var n syscall.Rlimit
	syscall.Getrlimit(syscall.RLIMIT_NOFILE, &n)
	return n.Cur
}
