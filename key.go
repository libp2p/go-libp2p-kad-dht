package dht

import (
	"math"
	"math/big"
	"strings"
)

var (
	// KeySpaceMax corresponds to the value 2^256-1 after init() was called. This value is used
	// to norm XOR distances to the range [0,1]. The maximum distance between two points in the
	// key space is KeySpaceMax. Therefore, dividing a distance by KeySpaceMax norms it to
	// the above range of [0,1].
	KeySpaceMax *big.Int

	// extKeySpaceMax is the KeySpaceMax extended by normAccuracy.
	extKeySpaceMax *big.Int

	// normAccuracy captures the accuracy that the decimal norm value should have.
	normAccuracy = uint64(math.MaxUint64)
)

func init() {
	// Initializes the maximum possible value of a 256-bit integer => 2^256-1
	val, ok := new(big.Int).SetString(strings.Repeat("1", 256), 2)
	if !ok {
		panic("failed to initialize maximum XOR distance value")
	}
	KeySpaceMax = val
	extKeySpaceMax = new(big.Int).Mul(KeySpaceMax, new(big.Int).SetUint64(normAccuracy))
}

// NormDistance takes a distance value and norms it to the range [0, 1].
func NormDistance(distance *big.Int) float64 {
	if distance.Uint64() == 0 {
		return 0
	}
	div, _ := new(big.Float).SetInt(new(big.Int).Div(extKeySpaceMax, distance)).Float64()
	return float64(normAccuracy) / div
}
