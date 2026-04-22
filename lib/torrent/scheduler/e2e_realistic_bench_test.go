// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scheduler

import (
	"io"
	"sync"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"

	"github.com/stretchr/testify/require"
)

// Production blob/piece sizes. Kraken uses 4MB pieces for all blobs.
// Real workload: ~1GB blobs = 256 pieces. We use 16MB (4 pieces)
// to keep test memory reasonable while exercising the same code
// paths and per-piece overhead at production piece size.
const (
	realisticBlobSize  = 16 * 1024 * 1024 // 16 MB
	realisticPieceSize = 4 * 1024 * 1024  // 4 MB — production config
)

// BenchmarkRealistic_P2PDownload simulates an agent downloading a
// blob via P2P from a seeder. Uses production 4MB piece size.
//
// Measures: full P2P pipeline — tracker announce, handshake, piece
// dispatch, piece write (writeBufPool), hash verification, cache commit.
func BenchmarkRealistic_P2PDownload(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()
	seeder := mocks.newPeer(config)

	blob := core.SizedBlobFixture(realisticBlobSize, realisticPieceSize)

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	seeder.writeTorrent(namespace, blob)
	require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

	b.SetBytes(realisticBlobSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		leecher := mocks.newPeer(config)
		require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
		checkTorrentB(b, leecher, namespace, blob)
		leecher.scheduler.Stop()
	}

	b.StopTimer()
}

// BenchmarkRealistic_CacheHit simulates repeated pulls of a blob
// already in the agent's cache. After the first P2P download, every
// subsequent pull on the same host reads directly from the cache
// file — no scheduler, no P2P, no piece machinery.
//
// This is the dominant code path: downloadBlobHandler reads from
// cads.Cache().GetFileReader() and io.Copy's to the HTTP response.
// We benchmark the store read + io.Copy to measure that path.
func BenchmarkRealistic_CacheHit(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	peer := mocks.newPeer(config)

	blob := core.SizedBlobFixture(realisticBlobSize, realisticPieceSize)

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	// Pre-populate cache via P2P.
	peer.writeTorrent(namespace, blob)
	require.NoError(peer.scheduler.Download(namespace, blob.Digest))

	b.SetBytes(realisticBlobSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Exactly what downloadBlobHandler does on cache hit.
		f, err := peer.cads.Cache().GetFileReader(blob.Digest.Hex())
		require.NoError(err)
		n, err := io.Copy(io.Discard, f)
		require.NoError(err)
		require.NoError(f.Close())
		require.Equal(int64(realisticBlobSize), n)
	}

	b.StopTimer()
}

// BenchmarkRealistic_ConcurrentCacheHits simulates 10 containers
// pulling the same cached blob simultaneously — e.g. 10 pods
// starting on the same host at once.
//
// Measures: concurrent file descriptor handling, io.Copy under
// parallel load, OS page cache behavior.
func BenchmarkRealistic_ConcurrentCacheHits(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	peer := mocks.newPeer(config)

	blob := core.SizedBlobFixture(realisticBlobSize, realisticPieceSize)

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	peer.writeTorrent(namespace, blob)
	require.NoError(peer.scheduler.Download(namespace, blob.Digest))

	const concurrency = 10
	b.SetBytes(realisticBlobSize * concurrency)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		errs := make(chan error, concurrency)

		for j := 0; j < concurrency; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				f, err := peer.cads.Cache().GetFileReader(blob.Digest.Hex())
				if err != nil {
					errs <- err
					return
				}
				if _, err := io.Copy(io.Discard, f); err != nil {
					f.Close()
					errs <- err
					return
				}
				if err := f.Close(); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

// BenchmarkRealistic_P2PFirstPullThenCache simulates the full
// lifecycle of a blob on a single host:
// 1. First pull: cold P2P download from seeder
// 2. Subsequent pulls: hot cache reads (5x)
//
// This is the actual production pattern. The ratio matters:
// optimizations to the P2P path help only the first pull, while
// cache-path optimizations help every subsequent pull.
func BenchmarkRealistic_P2PFirstPullThenCache(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	seeder := mocks.newPeer(config)

	const numCacheHits = 5

	blob := core.SizedBlobFixture(realisticBlobSize, realisticPieceSize)

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	seeder.writeTorrent(namespace, blob)
	require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

	b.SetBytes(realisticBlobSize * (1 + numCacheHits))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Phase 1: Cold P2P download on a fresh agent.
		leecher := mocks.newPeer(config)
		require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
		checkTorrentB(b, leecher, namespace, blob)

		// Phase 2: Serve from cache (what downloadBlobHandler does).
		for j := 0; j < numCacheHits; j++ {
			f, err := leecher.cads.Cache().GetFileReader(blob.Digest.Hex())
			require.NoError(err)
			n, err := io.Copy(io.Discard, f)
			require.NoError(err)
			require.NoError(f.Close())
			require.Equal(int64(realisticBlobSize), n)
		}

		leecher.scheduler.Stop()
	}

	b.StopTimer()
}

// BenchmarkRealistic_MultiPeerSwarm simulates 10 agents all pulling
// the same blob concurrently from 1 seeder — the burst scenario
// (production peak: 20K blobs in 30 seconds).
//
// Measures: tracker coordination, connection management, piece
// distribution efficiency, resource contention across many peers.
func BenchmarkRealistic_MultiPeerSwarm(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	seeder := mocks.newPeer(config)

	const numLeechers = 10

	blob := core.SizedBlobFixture(realisticBlobSize, realisticPieceSize)

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	seeder.writeTorrent(namespace, blob)
	require.NoError(seeder.scheduler.Download(namespace, blob.Digest))

	b.SetBytes(realisticBlobSize * numLeechers)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		leechers := make([]*testPeer, numLeechers)
		for j := 0; j < numLeechers; j++ {
			leechers[j] = mocks.newPeer(config)
		}

		var wg sync.WaitGroup
		for _, l := range leechers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(l.scheduler.Download(namespace, blob.Digest))
				checkTorrentB(b, l, namespace, blob)
			}()
		}
		wg.Wait()

		for _, l := range leechers {
			l.scheduler.Stop()
		}
	}

	b.StopTimer()
}

// BenchmarkRealistic_MultiBlobBurst simulates an agent pulling 5
// different blobs concurrently — the layers of a Docker image all
// downloading in parallel from the same seeder.
//
// Measures: scheduler event loop under multi-torrent load,
// per-torrent resource isolation, concurrent piece writes to
// different files.
func BenchmarkRealistic_MultiBlobBurst(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	seeder := mocks.newPeer(config)

	const numBlobs = 5

	blobs := make([]*core.BlobFixture, numBlobs)
	for k := range blobs {
		blobs[k] = core.SizedBlobFixture(realisticBlobSize, realisticPieceSize)
		mocks.metaInfoClient.EXPECT().Download(
			namespace, blobs[k].Digest).Return(blobs[k].MetaInfo, nil).AnyTimes()
		seeder.writeTorrent(namespace, blobs[k])
		require.NoError(seeder.scheduler.Download(namespace, blobs[k].Digest))
	}

	b.SetBytes(realisticBlobSize * numBlobs)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		leecher := mocks.newPeer(config)

		var wg sync.WaitGroup
		for _, blob := range blobs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(leecher.scheduler.Download(namespace, blob.Digest))
				checkTorrentB(b, leecher, namespace, blob)
			}()
		}
		wg.Wait()
		leecher.scheduler.Stop()
	}

	b.StopTimer()
}

// BenchmarkRealistic_PieceWriteIsolated benchmarks just the piece
// write path with production-size 4MB pieces. No P2P, no scheduler
// overhead — pure storage write + hash verification.
//
// Use this to isolate writeBufPool impact from network/scheduler noise.
func BenchmarkRealistic_PieceWriteIsolated(b *testing.B) {
	require := require.New(b)

	mocks, cleanup := newTestMocks(b)
	defer cleanup()

	config := benchConfig()
	namespace := core.TagFixture()

	numPieces := realisticBlobSize / realisticPieceSize

	blob := core.SizedBlobFixture(realisticBlobSize, realisticPieceSize)

	mocks.metaInfoClient.EXPECT().Download(
		namespace, blob.Digest).Return(blob.MetaInfo, nil).AnyTimes()

	pieces := make([][]byte, numPieces)
	for j := 0; j < numPieces; j++ {
		start := int64(j) * blob.MetaInfo.PieceLength()
		end := start + blob.MetaInfo.GetPieceLength(j)
		pieces[j] = blob.Content[start:end]
	}

	b.SetBytes(realisticBlobSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		peer := mocks.newPeer(config)
		tor, err := peer.torrentArchive.CreateTorrent(namespace, blob.Digest)
		require.NoError(err)

		for j := 0; j < numPieces; j++ {
			require.NoError(tor.WritePiece(piecereader.NewBuffer(pieces[j]), j))
		}

		peer.scheduler.Stop()
	}

	b.StopTimer()
}
