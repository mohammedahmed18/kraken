package dockerregistry

import "testing"

func BenchmarkParsePath_Manifests(b *testing.B) {
	path := "/docker/registry/v2/repositories/library/alpine/_manifests/tags/latest/current/link"
	for i := 0; i < b.N; i++ {
		ParsePath(path)
	}
}

func BenchmarkParsePath_Blobs(b *testing.B) {
	path := "/docker/registry/v2/blobs/sha256/e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/data"
	for i := 0; i < b.N; i++ {
		ParsePath(path)
	}
}

func BenchmarkParsePath_Layers(b *testing.B) {
	path := "/docker/registry/v2/repositories/library/alpine/_layers/sha256/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/link"
	for i := 0; i < b.N; i++ {
		ParsePath(path)
	}
}

func BenchmarkGetRepo(b *testing.B) {
	path := "/docker/registry/v2/repositories/library/alpine/_manifests/tags/latest/current/link"
	for i := 0; i < b.N; i++ {
		GetRepo(path)
	}
}

func BenchmarkGetBlobDigest(b *testing.B) {
	path := "/docker/registry/v2/blobs/sha256/e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/data"
	for i := 0; i < b.N; i++ {
		GetBlobDigest(path)
	}
}

func BenchmarkGetManifestTag(b *testing.B) {
	path := "/docker/registry/v2/repositories/library/alpine/_manifests/tags/latest/current/link"
	for i := 0; i < b.N; i++ {
		GetManifestTag(path)
	}
}
