package tiny

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func ExampleNode_String() {
	n := Node{key: 0b11111111}
	fmt.Println(n.String())

	n = Node{key: 0b01010101}
	fmt.Println(n.String())

	// Output:
	// ff
	// 55
}

func TestNodeWithCpl(t *testing.T) {
	testCases := []Key{Key(1), Key(2), Key(4), Key(8), Key(16), Key(32), Key(16), Key(128), Key(33), Key(159), Key(0), Key(255)}

	for _, k := range testCases {
		for cpl := 0; cpl < 8; cpl++ {
			n, err := NodeWithCpl(k, cpl)
			assert.NoError(t, err)
			assert.Equal(t, cpl, k.CommonPrefixLength(n.Key()))
		}
	}
}
