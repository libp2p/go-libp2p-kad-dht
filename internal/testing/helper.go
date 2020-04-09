package testing

import (
	"bytes"
	"errors"
)

type TestValidator struct{}

func (TestValidator) Select(_ string, bs [][]byte) (int, error) {
	index := -1
	for i, b := range bs {
		if bytes.Equal(b, []byte("newer")) {
			index = i
		} else if bytes.Equal(b, []byte("valid")) {
			if index == -1 {
				index = i
			}
		}
	}
	if index == -1 {
		return -1, errors.New("no rec found")
	}
	return index, nil
}
func (TestValidator) Validate(_ string, b []byte) error {
	if bytes.Equal(b, []byte("expired")) {
		return errors.New("expired")
	}
	return nil
}
