package core

import "testing"

const testHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
const testRaw = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func BenchmarkValidateSHA256(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ValidateSHA256(testHex)
	}
}

func BenchmarkNewSHA256DigestFromHex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NewSHA256DigestFromHex(testHex)
	}
}

func BenchmarkParseSHA256Digest(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ParseSHA256Digest(testRaw)
	}
}

func BenchmarkDigestString(b *testing.B) {
	d, _ := NewSHA256DigestFromHex(testHex)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.String()
	}
}

func BenchmarkDigestHex(b *testing.B) {
	d, _ := NewSHA256DigestFromHex(testHex)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.Hex()
	}
}

func BenchmarkDigestShardID(b *testing.B) {
	d, _ := NewSHA256DigestFromHex(testHex)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.ShardID()
	}
}
