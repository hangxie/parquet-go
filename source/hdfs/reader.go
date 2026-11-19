package hdfs

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/colinmarc/hdfs/v2"

	"github.com/hangxie/parquet-go/v3/source"
)

var _ source.ParquetFileReader = (*hdfsReader)(nil)

type hdfsReader struct {
	pool   *readerPool
	path   string
	size   int64
	isDir  bool
	idle   *list.Element // protected by the pool mutex
	mu     sync.Mutex
	offset int64
	closed bool
}

// NewHdfsFileReader opens an HDFS reader with at most 256 data streams per reader family.
func NewHdfsFileReader(hosts []string, user, name string) (source.ParquetFileReader, error) {
	return NewHdfsFileReaderWithStreamLimit(hosts, user, name, defaultStreamLimit)
}

// NewHdfsFileReaderWithStreamLimit bounds open streams across clones and explicitly opened paths.
func NewHdfsFileReaderWithStreamLimit(hosts []string, user, name string, limit int) (source.ParquetFileReader, error) {
	if limit < 1 {
		return nil, fmt.Errorf("HDFS stream limit must be positive: %d", limit)
	}
	client, err := hdfs.NewClient(hdfs.ClientOptions{Addresses: hosts, User: user})
	if err != nil {
		return nil, fmt.Errorf("new hdfs client: %w", err)
	}
	return newReaderPool(&realHdfsClient{client}, limit).open(name)
}

// open consumes a reserved client reference, releasing it if the metadata lookup fails.
func (p *readerPool) open(name string) (source.ParquetFileReader, error) {
	info, err := p.client.Stat(name)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("stat hdfs file %q: %w", name, err), p.release())
	}
	return &hdfsReader{pool: p, path: name, size: info.Size(), isDir: info.IsDir()}, nil
}

func (f *hdfsReader) Open(name string) (source.ParquetFileReader, error) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, os.ErrClosed
	}
	f.pool.retain()
	f.mu.Unlock()
	return f.pool.open(name)
}

func (f *hdfsReader) Clone() (source.ParquetFileReader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, os.ErrClosed
	}
	f.pool.retain()
	return &hdfsReader{pool: f.pool, path: f.path, size: f.size, isDir: f.isDir}, nil
}

func (f *hdfsReader) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += f.offset
	case io.SeekEnd:
		offset += f.size
	default:
		return f.offset, fmt.Errorf("invalid whence: %d", whence)
	}
	if offset < 0 || offset > f.size {
		return f.offset, fmt.Errorf("invalid resulting offset: %d", offset)
	}
	f.offset = offset
	return offset, nil
}

func (f *hdfsReader) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if f.isDir {
		return 0, &os.PathError{Op: "read", Path: f.path, Err: errors.New("is a directory")}
	}
	if f.offset == f.size {
		return 0, io.EOF
	}
	h, err := f.pool.acquire(f)
	if err != nil {
		return 0, err
	}
	// Even a seek to the current offset can disconnect the upstream block reader.
	if h.offset != f.offset {
		if _, err = h.file.Seek(f.offset, io.SeekStart); err != nil {
			return 0, errors.Join(err, f.pool.closeHandle(h))
		}
	}
	remaining := f.size - f.offset
	pastEnd := int64(len(p)) > remaining
	if pastEnd {
		p = p[:remaining]
	}
	n, err := io.ReadFull(h.file, p)
	f.offset += int64(n)
	h.offset = f.offset
	if (pastEnd && err == nil) || errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	if err != nil || f.offset >= f.size {
		if closeErr := f.pool.closeHandle(h); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	} else {
		f.pool.put(h)
	}
	return n, err
}

func (f *hdfsReader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	return errors.Join(f.pool.discard(f), f.pool.release())
}
