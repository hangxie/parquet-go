package swiftsource

import (
	"container/list"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/ncw/swift"
)

const defaultStreamLimit = 256

type streamEntry struct {
	file     *swift.ObjectOpenFile
	position int64
	idle     *list.Element
}

type readerPool struct {
	limit      int
	mu         sync.Mutex
	cond       *sync.Cond
	connection *swift.Connection
	container  string
	streams    map[*streamCursor]*streamEntry
	idle       list.List
	slots      int // includes streams being opened or closed
}

func newReaderPool(connection *swift.Connection, container string, limit int) *readerPool {
	p := &readerPool{limit: limit, connection: connection, container: container, streams: make(map[*streamCursor]*streamEntry)}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// acquire pins a cursor's stream until release, opening only after a slot is reserved.
func (p *readerPool) acquire(c *streamCursor) (*streamEntry, error) {
	p.mu.Lock()
	var victim *streamEntry
	for {
		if e := p.streams[c]; e != nil {
			p.idle.Remove(e.idle)
			e.idle = nil
			if e.position == c.position {
				p.mu.Unlock()
				return e, nil
			}
			delete(p.streams, c)
			victim = e
			break
		}
		if p.slots < p.limit {
			p.slots++
			break
		}
		if oldest := p.idle.Front(); oldest != nil {
			owner := oldest.Value.(*streamCursor)
			victim = p.streams[owner]
			delete(p.streams, owner)
			p.idle.Remove(oldest)
			break
		}
		// Swift currently relies on backend timeouts; waiting is not context-aware.
		p.cond.Wait()
	}
	p.mu.Unlock()

	// Backend I/O never holds the pool lock. Other pinned streams keep reading.
	if victim != nil {
		// Eviction must not charge another cursor for closing a discarded body.
		_ = victim.file.Close()
	}
	headers := swift.Headers{"If-Match": fmt.Sprintf("%q", c.etag)}
	if c.position > 0 {
		headers["Range"] = fmt.Sprintf("bytes=%d-", c.position)
	}
	file, responseHeaders, err := p.connection.ObjectOpen(p.container, c.path, false, headers)
	if err != nil {
		if file != nil {
			_ = file.Close()
		}
		p.freeSlot()
		var swiftErr *swift.Error
		if errors.As(err, &swiftErr) && swiftErr.StatusCode == http.StatusPreconditionFailed {
			return nil, fmt.Errorf("swift object changed while reading %q: %w", c.path, err)
		}
		return nil, fmt.Errorf("swift object open: %w", err)
	}
	// Also validate the response: gateways may ignore a conditional request.
	if strings.Trim(responseHeaders["Etag"], "\"") != c.etag {
		_ = file.Close()
		p.freeSlot()
		return nil, fmt.Errorf("swift object changed while reading %q", c.path)
	}
	if c.position > 0 && !validContentRange(responseHeaders["Content-Range"], c.position, c.size) {
		_ = file.Close()
		p.freeSlot()
		return nil, fmt.Errorf("invalid Swift Content-Range %q: expected bytes %d-%d/%d", responseHeaders["Content-Range"], c.position, c.size-1, c.size)
	}
	e := &streamEntry{file: file, position: c.position}
	p.mu.Lock()
	p.streams[c] = e
	p.mu.Unlock()
	return e, nil
}

func (p *readerPool) freeSlot() {
	p.mu.Lock()
	p.slots--
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *readerPool) release(c *streamCursor, e *streamEntry) {
	p.mu.Lock()
	e.idle = p.idle.PushBack(c)
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *readerPool) discard(c *streamCursor) error {
	p.mu.Lock()
	e := p.streams[c]
	if e == nil {
		p.mu.Unlock()
		return nil
	}
	delete(p.streams, c)
	if e.idle != nil {
		p.idle.Remove(e.idle)
	}
	p.mu.Unlock()
	err := e.file.Close()
	p.freeSlot()
	return err
}

// validContentRange requires the complete suffix requested by bytes=position-.
func validContentRange(value string, position, size int64) bool {
	if position < 0 || position >= size {
		return false
	}
	spec, ok := strings.CutPrefix(value, "bytes ")
	if !ok {
		return false
	}
	bounds, totalText, ok := strings.Cut(spec, "/")
	if !ok {
		return false
	}
	startText, endText, ok := strings.Cut(bounds, "-")
	if !ok {
		return false
	}
	start, startErr := strconv.ParseUint(startText, 10, 63)
	end, endErr := strconv.ParseUint(endText, 10, 63)
	total, totalErr := strconv.ParseUint(totalText, 10, 63)
	return startErr == nil && endErr == nil && totalErr == nil && start == uint64(position) && end == uint64(size-1) && total == uint64(size)
}
