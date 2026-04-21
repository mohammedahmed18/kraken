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
package piecereader

import (
	"bytes"
	"io"
	"sync"
)

// Buffer is a storage.PieceReader which reads a piece from an in-memory buffer.
type Buffer struct {
	reader *bytes.Reader
}

// NewBuffer returns a new Buffer for b.
func NewBuffer(b []byte) *Buffer {
	return &Buffer{bytes.NewReader(b)}
}

// Read reads a piece into p.
func (b *Buffer) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

// WriteTo implements io.WriterTo. This allows io.Copy to avoid
// allocating an intermediate buffer when copying piece data.
func (b *Buffer) WriteTo(w io.Writer) (int64, error) {
	return b.reader.WriteTo(w)
}

// Close noops.
func (b *Buffer) Close() error {
	return nil
}

// Length returns the length of the piece.
func (b *Buffer) Length() int {
	return b.reader.Len()
}

// payloadPool pools byte slices for piece payloads. Piece sizes
// are typically uniform within a torrent, so pooling is effective.
var payloadPool = sync.Pool{}

// PooledBuffer is a Buffer backed by a pooled byte slice. The
// slice is returned to the pool when Close is called. Callers
// must not use the buffer after closing.
type PooledBuffer struct {
	Buffer
	buf *[]byte
}

// NewPooledBuffer returns a PooledBuffer for reading piece data
// of the given length. The backing slice comes from a pool.
func NewPooledBuffer(length int) *PooledBuffer {
	var bp *[]byte
	if v := payloadPool.Get(); v != nil {
		bp = v.(*[]byte)
		if cap(*bp) < length {
			b := make([]byte, length)
			bp = &b
		} else {
			*bp = (*bp)[:length]
		}
	} else {
		b := make([]byte, length)
		bp = &b
	}
	return &PooledBuffer{
		Buffer: Buffer{reader: bytes.NewReader(*bp)},
		buf:    bp,
	}
}

// Bytes returns the underlying byte slice for writing into.
func (pb *PooledBuffer) Bytes() []byte {
	return *pb.buf
}

// Reset re-initializes the reader after the slice has been
// written to. Must be called after filling Bytes().
func (pb *PooledBuffer) Reset() {
	pb.reader.Reset(*pb.buf)
}

// Close returns the backing slice to the pool.
func (pb *PooledBuffer) Close() error {
	if pb.buf != nil {
		payloadPool.Put(pb.buf)
		pb.buf = nil
	}
	return nil
}
