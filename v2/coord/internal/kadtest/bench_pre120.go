//go:build !go1.20

package kadtest

import "testing"

// ReportTimePerItemMetric is a no-op on versions of Go before 1.20
func ReportTimePerItemMetric(b *testing.B, n int, name string) {
	// no-op
}
