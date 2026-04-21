package hrw

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
)

func BenchmarkGetOrderedNodes_4Nodes(b *testing.B) {
	rh := NewRendezvousHash(Murmur3Hash, UInt64ToFloat64)
	rh.AddNode("node0", 100)
	rh.AddNode("node1", 200)
	rh.AddNode("node2", 400)
	rh.AddNode("node3", 800)

	buf := make([]byte, 32)
	rand.Read(buf)
	key := hex.EncodeToString(buf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rh.GetOrderedNodes(key, 1)
	}
}

func BenchmarkGetOrderedNodes_10Nodes(b *testing.B) {
	rh := NewRendezvousHash(Murmur3Hash, UInt64ToFloat64)
	for i := 0; i < 10; i++ {
		rh.AddNode("node"+string(rune('0'+i)), 100+i*50)
	}

	buf := make([]byte, 32)
	rand.Read(buf)
	key := hex.EncodeToString(buf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rh.GetOrderedNodes(key, 3)
	}
}

func BenchmarkScore(b *testing.B) {
	rh := NewRendezvousHash(Murmur3Hash, UInt64ToFloat64)
	rh.AddNode("node0", 100)

	buf := make([]byte, 32)
	rand.Read(buf)
	key := hex.EncodeToString(buf)

	node := rh.Nodes[0]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node.Score(key)
	}
}
