package netsize

import (
	"fmt"
	"math"

	kbucket "github.com/libp2p/go-libp2p-kbucket"
)

type WeightFuncType string

const (
	WeightFuncNone                   WeightFuncType = "NONE"
	WeightFuncInverse                WeightFuncType = "INVERSE"
	WeightFuncExponentialCPL         WeightFuncType = "EXPONENTIAL_CPL"
	WeightFuncExponentialBucketLevel WeightFuncType = "EXPONENTIAL_BUCKET_LEVEL"
)

// calcWeight selects the configured weight function and applies and calculates
// the weight of the data points.
func (e *Estimator) calcWeight(key string) float64 {
	switch e.weightFuncType {
	case WeightFuncNone:
		return e.weightFuncNone(key)
	case WeightFuncInverse:
		return e.weightFuncInverse(key)
	case WeightFuncExponentialCPL:
		return e.weightFuncExponentialCPL(key)
	case WeightFuncExponentialBucketLevel:
		return e.weightFuncExponentialBucketLevel(key)
	default:
		panic(fmt.Sprintf("unknown weight func type %s", e.weightFuncType))
	}
}

// weightFuncNone does not weigh data points but treats every sample the same.
func (e *Estimator) weightFuncNone(key string) float64 {
	return 1
}

// weightFuncInverse decreases the weight of data points inverse proportional to its
// common prefix length between the requested key and the own peer ID.
// CPL: 0 -> 1/(0 + 1) -> 1
// CPL: 1 -> 1/(1 + 1) -> 0.5
// CPL: 2 -> 1/(2 + 1) -> 0.333
// CPL: 3 -> 1/(3 + 1) -> 0.25
func (e *Estimator) weightFuncInverse(key string) float64 {
	cpl := kbucket.CommonPrefixLen(kbucket.ConvertKey(key), e.localID)
	return 1 / (float64(cpl) + 1)
}

// weightFuncExponentialCPL is similar to weightFuncInverse but instead of applying
// an inverse proportional relationship it weighs data points exponentially less
// with increasing common prefix length between the requested key and the own peer ID.
// CPL: 0 -> 1/2**0 -> 1
// CPL: 1 -> 1/2**1 -> 0.5
// CPL: 2 -> 1/2**2 -> 0.25
// CPL: 3 -> 1/2**3 -> 0.125
func (e *Estimator) weightFuncExponentialCPL(key string) float64 {
	cpl := kbucket.CommonPrefixLen(kbucket.ConvertKey(key), e.localID)
	return 1 / math.Pow(2, float64(cpl))
}

// weightFuncExponentialBucketLevel weighs data points exponentially less if they fall into a non-full bucket.
// It weighs distance estimates based on their CPLs and bucket levels.
// Bucket Level: 20 -> 1/2^0 -> weight: 1
// Bucket Level: 17 -> 1/2^3 -> weight: 1/8
// Bucket Level: 10 -> 1/2^10 -> weight: 1/1024
func (e *Estimator) weightFuncExponentialBucketLevel(key string) float64 {
	cpl := kbucket.CommonPrefixLen(kbucket.ConvertKey(key), e.localID)
	bucketLevel := e.rt.NPeersForCpl(uint(cpl))
	return math.Pow(2, float64(bucketLevel-e.bucketSize))
}
