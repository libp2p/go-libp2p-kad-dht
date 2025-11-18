//go:build go1.20

package kadtest

import "testing"

// ReportTimePerItemMetric adds a custom metric to a benchmark that reports the number of nanoseconds taken per item.
func ReportTimePerItemMetric(b *testing.B, n int, name string) {
	// b.Elapsed was added in Go 1.20
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/"+name)
}
