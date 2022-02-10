package dht

import (
	"fmt"
	"math/big"
)

func ExampleKeySpaceMax() {
	fmt.Println(KeySpaceMax)
	// Output:
	// 115792089237316195423570985008687907853269984665640564039457584007913129639935
}

func ExampleNormDistance() {
	fmt.Println(NormDistance(KeySpaceMax))
	fmt.Println(NormDistance(new(big.Int).Div(KeySpaceMax, big.NewInt(2))))
	fmt.Println(NormDistance(new(big.Int).Div(KeySpaceMax, big.NewInt(4))))
	fmt.Println(NormDistance(big.NewInt(0)))
	// Output:
	// 1
	// 0.5
	// 0.25
	// 0
}
