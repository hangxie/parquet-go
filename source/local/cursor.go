package local

import (
	"fmt"
	"io"
	"math"
	"os"
	"sync"
)

type sharedFile struct {
	sync.RWMutex
	file *os.File
	refs int
}

// fileCursor owns one reference and an independent offset in a local file.
type fileCursor struct {
	mu     sync.Mutex
	shared *sharedFile
	offset int64
	closed bool
}

// newFileCursor takes ownership of an os.File, whose ReadAt supports concurrent calls.
func newFileCursor(file *os.File) *fileCursor {
	return &fileCursor{shared: &sharedFile{file: file, refs: 1}}
}

// Clone creates a cursor at offset zero without opening another handle.
func (c *fileCursor) Clone() (*fileCursor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, os.ErrClosed
	}
	c.shared.Lock()
	defer c.shared.Unlock()
	c.shared.refs++
	return &fileCursor{shared: c.shared}, nil
}

func (c *fileCursor) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, os.ErrClosed
	}
	s := c.shared
	s.RLock()
	defer s.RUnlock()
	n, err := s.file.ReadAt(p, c.offset)
	c.offset += int64(n)
	return n, err
}

func (c *fileCursor) Seek(offset int64, whence int) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, os.ErrClosed
	}

	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = c.offset
	case io.SeekEnd:
		info, err := c.shared.file.Stat()
		if err != nil {
			return 0, err
		}
		base = info.Size()
	default:
		return 0, fmt.Errorf("invalid seek whence: %d", whence)
	}
	if offset > math.MaxInt64-base {
		return 0, fmt.Errorf("seek offset overflows int64")
	}
	pos := base + offset
	if pos < 0 {
		return 0, fmt.Errorf("negative seek offset: %d", pos)
	}
	c.offset = pos
	return pos, nil
}

// Close releases this cursor and closes the handle after its last reference.
func (c *fileCursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	s := c.shared
	s.Lock()
	defer s.Unlock()
	s.refs--
	if s.refs == 0 {
		return s.file.Close()
	}
	return nil
}
