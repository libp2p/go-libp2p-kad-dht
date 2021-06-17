package fullrt

import (
	"strconv"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestDivideByChunkSize(t *testing.T) {
	var keys []peer.ID
	for i := 0; i < 10; i++ {
		keys = append(keys, peer.ID(strconv.Itoa(i)))
	}

	convertToStrings := func(peers []peer.ID) []string {
		var out []string
		for _, p := range peers {
			out = append(out, string(p))
		}
		return out
	}

	pidsEquals := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i, v := range a {
			if v != b[i] {
				return false
			}
		}
		return true
	}

	t.Run("Divides", func(t *testing.T) {
		gr := divideByChunkSize(keys, 5)
		if len(gr) != 2 {
			t.Fatal("incorrect number of groups")
		}
		if g1, expected := convertToStrings(gr[0]), []string{"0", "1", "2", "3", "4"}; !pidsEquals(g1, expected) {
			t.Fatalf("expected %v, got %v", expected, g1)
		}
		if g2, expected := convertToStrings(gr[1]), []string{"5", "6", "7", "8", "9"}; !pidsEquals(g2, expected) {
			t.Fatalf("expected %v, got %v", expected, g2)
		}
	})
	t.Run("Remainder", func(t *testing.T) {
		gr := divideByChunkSize(keys, 3)
		if len(gr) != 4 {
			t.Fatal("incorrect number of groups")
		}
		if g, expected := convertToStrings(gr[0]), []string{"0", "1", "2"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
		if g, expected := convertToStrings(gr[1]), []string{"3", "4", "5"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
		if g, expected := convertToStrings(gr[2]), []string{"6", "7", "8"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
		if g, expected := convertToStrings(gr[3]), []string{"9"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
	})
	t.Run("OneEach", func(t *testing.T) {
		gr := divideByChunkSize(keys, 1)
		if len(gr) != 10 {
			t.Fatal("incorrect number of groups")
		}
		for i := 0; i < 10; i++ {
			if g, expected := convertToStrings(gr[i]), []string{strconv.Itoa(i)}; !pidsEquals(g, expected) {
				t.Fatalf("expected %v, got %v", expected, g)
			}
		}
	})
	t.Run("ChunkSizeLargerThanKeys", func(t *testing.T) {
		gr := divideByChunkSize(keys, 11)
		if len(gr) != 1 {
			t.Fatal("incorrect number of groups")
		}
		if g, expected := convertToStrings(gr[0]), convertToStrings(keys); !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
	})
}
