package swiftsource

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
)

type streamCursor struct {
	mu       sync.Mutex
	pool     *readerPool
	path     string
	position int64
	size     int64
	etag     string
	closed   bool
}

func newStreamCursor(pool *readerPool, path string) (*streamCursor, error) {
	info, _, err := pool.connection.Object(pool.container, path)
	if err != nil {
		return nil, fmt.Errorf("swift object metadata: %w", err)
	}
	etag := strings.Trim(info.Hash, "\"")
	if etag == "" {
		return nil, fmt.Errorf("swift object ETag is missing")
	}
	if info.Bytes < 0 {
		return nil, fmt.Errorf("swift object size is negative: %d", info.Bytes)
	}
	return &streamCursor{pool: pool, path: path, size: info.Bytes, etag: etag}, nil
}

func (c *streamCursor) Clone() (*streamCursor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, os.ErrClosed
	}
	return &streamCursor{pool: c.pool, path: c.path, size: c.size, etag: c.etag}, nil
}

func (c *streamCursor) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, os.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if c.position >= c.size {
		return 0, io.EOF
	}
	e, err := c.pool.acquire(c)
	if err != nil {
		return 0, err
	}
	n, err := e.file.Read(p)
	c.position += int64(n)
	e.position = c.position
	if err != nil || c.position >= c.size {
		if closeErr := c.pool.discard(c); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close Swift stream: %w", closeErr))
		}
	} else {
		c.pool.release(c, e)
	}
	return n, err
}

func (c *streamCursor) Seek(offset int64, whence int) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, os.ErrClosed
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = c.position
	case io.SeekEnd:
		base = c.size
	default:
		return 0, fmt.Errorf("invalid seek whence: %d", whence)
	}
	if offset > math.MaxInt64-base {
		return 0, fmt.Errorf("seek offset overflows int64")
	}
	position := base + offset
	if position < 0 {
		return 0, fmt.Errorf("negative seek offset: %d", position)
	}
	c.position = position
	return position, nil
}

func (c *streamCursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.pool.discard(c)
}
