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
	"fmt"
	"io"
	"sync"

	"github.com/uber/kraken/lib/store"
)

// copyBufPool is a pool of 32KB buffers used by FileReader.WriteTo
// to avoid allocating a new buffer for every io.Copy call during
// piece transfers.
var copyBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 32*1024)
		return &b
	},
}

// Opener opens files.
type Opener interface {
	Open() (store.FileReader, error)
}

// FileReader is a storage.PieceReader which reads a piece from a file.
type FileReader struct {
	offset int64
	length int64

	opener Opener
	closer io.Closer
	reader io.Reader
}

// NewFileReader creates a FileReader which reads a piece from f. f should not
// be used once it is given to a FileReader.
func NewFileReader(offset, length int64, opener Opener) *FileReader {
	return &FileReader{
		offset: offset,
		length: length,
		opener: opener,
	}
}

// init lazily opens the file and prepares the reader.
func (r *FileReader) init() error {
	if r.reader != nil {
		return nil
	}
	f, err := r.opener.Open()
	if err != nil {
		return fmt.Errorf("open: %s", err)
	}
	if _, err := f.Seek(r.offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek: %s", err)
	}
	r.reader = io.LimitReader(f, r.length)
	r.closer = f
	return nil
}

// Read reads a piece in p.
func (r *FileReader) Read(p []byte) (int, error) {
	if err := r.init(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

// WriteTo implements io.WriterTo. This copies using a pooled
// buffer via an explicit read/write loop. We avoid io.Copy and
// io.CopyBuffer here because they check for ReadFrom on the
// destination, which for *net.TCPConn triggers sendFile and
// allocates internally, defeating the pooled buffer.
func (r *FileReader) WriteTo(w io.Writer) (int64, error) {
	if err := r.init(); err != nil {
		return 0, err
	}
	bp := copyBufPool.Get().(*[]byte)
	buf := *bp
	defer copyBufPool.Put(bp)
	var written int64
	for {
		nr, er := r.reader.Read(buf)
		if nr > 0 {
			nw, ew := w.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			if er != io.EOF {
				return written, er
			}
			break
		}
	}
	return written, nil
}

// Close closes the underlying file.
func (r *FileReader) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}

// Length returns the length of the piece.
func (r *FileReader) Length() int {
	return int(r.length)
}
