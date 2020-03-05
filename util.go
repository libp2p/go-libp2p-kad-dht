package dht

// PoolSize is the number of nodes used for group find/set RPC calls
// Deprecated: No longer used
var PoolSize = 6

// KValue is the maximum number of requests to perform before returning failure.
// Deprecated: This value is still the default, but changing this parameter no longer does anything.
var KValue = 20

// AlphaValue is the concurrency factor for asynchronous requests.
// Deprecated: This value is still the default, but changing this parameter no longer does anything.
var AlphaValue = 3
