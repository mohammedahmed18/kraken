package namepath

import "testing"

func BenchmarkDockerTagPather_NameFromBlobPath(b *testing.B) {
	p := DockerTagPather{root: "/root"}
	bp := "/root/docker/registry/v2/repositories/library/alpine/_manifests/tags/latest/current/link"
	for i := 0; i < b.N; i++ {
		p.NameFromBlobPath(bp)
	}
}

func BenchmarkShardedDockerBlobPather_NameFromBlobPath(b *testing.B) {
	p := ShardedDockerBlobPather{root: "/root"}
	bp := "/root/docker/registry/v2/blobs/sha256/e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/data"
	for i := 0; i < b.N; i++ {
		p.NameFromBlobPath(bp)
	}
}

func BenchmarkDockerTagPather_BlobPath(b *testing.B) {
	p := DockerTagPather{root: "/root"}
	for i := 0; i < b.N; i++ {
		p.BlobPath("library/alpine:latest")
	}
}
