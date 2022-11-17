package netsize

type Option func(*Estimator)

var (
	DefaultBucketSize     = 20
	DefaultWeightFuncType = WeightFuncExponentialCPL
)

func WithBucketSize(bucketSize int) Option {
	return func(es *Estimator) {
		es.bucketSize = bucketSize
	}
}

func WithWeightFunc(wft WeightFuncType) Option {
	return func(es *Estimator) {
		es.weightFuncType = wft
	}
}
