package tiny

import (
	"fmt"
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
