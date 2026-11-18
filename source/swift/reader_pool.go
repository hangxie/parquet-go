package swiftsource

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/ncw/swift"
)

type streamEntry struct {
	file     *swift.ObjectOpenFile
	position int64
}

type readerPool struct {
	mu         sync.Mutex
	connection *swift.Connection
	container  string
	streams    map[*streamCursor]*streamEntry
}

func newReaderPool(connection *swift.Connection, container string) *readerPool {
	return &readerPool{connection: connection, container: container, streams: make(map[*streamCursor]*streamEntry)}
}

// acquire reuses the cursor's stream unless its position changed through Seek.
func (p *readerPool) acquire(c *streamCursor) (*streamEntry, error) {
	p.mu.Lock()
	previous := p.streams[c]
	if previous != nil && previous.position == c.position {
		p.mu.Unlock()
		return previous, nil
	}
	delete(p.streams, c)
	p.mu.Unlock()
	if previous != nil {
		_ = previous.file.Close()
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
		var swiftErr *swift.Error
		if errors.As(err, &swiftErr) && swiftErr.StatusCode == http.StatusPreconditionFailed {
			return nil, fmt.Errorf("swift object changed while reading %q: %w", c.path, err)
		}
		return nil, fmt.Errorf("swift object open: %w", err)
	}
	// Also validate the response: gateways may ignore a conditional request.
	if strings.Trim(responseHeaders["Etag"], "\"") != c.etag {
		_ = file.Close()
		return nil, fmt.Errorf("swift object changed while reading %q", c.path)
	}
	if c.position > 0 && !validContentRange(responseHeaders["Content-Range"], c.position, c.size) {
		_ = file.Close()
		return nil, fmt.Errorf("invalid Swift Content-Range %q: expected bytes %d-%d/%d", responseHeaders["Content-Range"], c.position, c.size-1, c.size)
	}

	e := &streamEntry{file: file, position: c.position}
	p.mu.Lock()
	p.streams[c] = e
	p.mu.Unlock()
	return e, nil
}

func (p *readerPool) discard(c *streamCursor) error {
	p.mu.Lock()
	e := p.streams[c]
	delete(p.streams, c)
	p.mu.Unlock()
	if e == nil {
		return nil
	}
	return e.file.Close()
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
