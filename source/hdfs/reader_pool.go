package hdfs

import (
	"container/list"
	"fmt"
	"sync"
)

const defaultStreamLimit = 256

type readerPool struct {
	client hdfsClientIface
	limit  int
	mu     sync.Mutex
	ready  *sync.Cond
	refs   int // includes pending Open calls
	slots  int // includes handles opening, reading, idle, or closing
	idle   list.List
}

type readerHandle struct {
	owner  *hdfsReader
	file   hdfsFileReaderIface
	offset int64
}

func newReaderPool(client hdfsClientIface, limit int) *readerPool {
	p := &readerPool{client: client, limit: limit, refs: 1}
	p.ready = sync.NewCond(&p.mu)
	return p
}

func (p *readerPool) retain() {
	p.mu.Lock()
	p.refs++
	p.mu.Unlock()
}

func (p *readerPool) release() error {
	p.mu.Lock()
	p.refs--
	last := p.refs == 0
	p.mu.Unlock()
	if last {
		if err := p.client.Close(); err != nil {
			return fmt.Errorf("close HDFS client: %w", err)
		}
	}
	return nil
}

// takeIdle removes a handle from the LRU while the pool lock is held.
func (p *readerPool) takeIdle(e *list.Element) *readerHandle {
	h := p.idle.Remove(e).(*readerHandle)
	h.owner.idle = nil
	return h
}

// acquire pins the caller's handle or reserves a slot before opening a new one.
func (p *readerPool) acquire(r *hdfsReader) (*readerHandle, error) {
	p.mu.Lock()
	if r.idle != nil {
		h := p.takeIdle(r.idle)
		p.mu.Unlock()
		return h, nil
	}
	for p.slots == p.limit && p.idle.Len() == 0 {
		p.ready.Wait()
	}
	var victim *readerHandle
	if p.slots == p.limit {
		victim = p.takeIdle(p.idle.Front())
	} else {
		p.slots++
	}
	p.mu.Unlock()

	if victim != nil {
		// Eviction discards another cursor's idle stream. Its close error is unrelated to this read.
		_ = victim.file.Close()
	}
	file, err := p.client.Open(r.path)
	if err != nil {
		p.freeSlot()
		return nil, fmt.Errorf("open hdfs file %q: %w", r.path, err)
	}
	return &readerHandle{owner: r, file: file}, nil
}

func (p *readerPool) put(h *readerHandle) {
	p.mu.Lock()
	h.owner.idle = p.idle.PushBack(h)
	p.ready.Signal()
	p.mu.Unlock()
}

func (p *readerPool) freeSlot() {
	p.mu.Lock()
	p.slots--
	p.ready.Signal()
	p.mu.Unlock()
}

func (p *readerPool) closeHandle(h *readerHandle) error {
	err := h.file.Close()
	p.freeSlot()
	if err != nil {
		return fmt.Errorf("close HDFS file reader: %w", err)
	}
	return nil
}

func (p *readerPool) discard(r *hdfsReader) error {
	p.mu.Lock()
	if r.idle == nil {
		p.mu.Unlock()
		return nil
	}
	h := p.takeIdle(r.idle)
	p.mu.Unlock()
	return p.closeHandle(h)
}
