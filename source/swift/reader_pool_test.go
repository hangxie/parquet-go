package swiftsource

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/ncw/swift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
)

type requestTracker struct {
	getStatus                   int
	rangeMode                   string
	badGETLength                bool
	heads                       int
	matches                     []string
	entered                     chan struct{}
	unblock                     chan struct{}
	failHead, closeErr, readErr bool
	badMetadata                 string
	mu                          sync.Mutex
	gets, active, peak          int
	fail                        bool
	ranges                      []string
}

type trackedBody struct {
	io.ReadCloser
	once     sync.Once
	readOnce sync.Once
	tracker  *requestTracker
}

func (b *trackedBody) Close() error {
	err := b.ReadCloser.Close()
	b.tracker.mu.Lock()
	fail := b.tracker.closeErr
	b.tracker.mu.Unlock()
	if fail {
		err = errors.New("injected close failure")
	}
	b.once.Do(func() { b.tracker.mu.Lock(); b.tracker.active--; b.tracker.mu.Unlock() })
	return err
}

func (b *trackedBody) Read(p []byte) (int, error) {
	b.readOnce.Do(func() {
		if b.tracker.entered != nil {
			b.tracker.entered <- struct{}{}
			<-b.tracker.unblock
		}
	})
	b.tracker.mu.Lock()
	fail := b.tracker.readErr
	b.tracker.mu.Unlock()
	if fail {
		return 0, errors.New("injected read failure")
	}
	return b.ReadCloser.Read(p)
}

func (t *requestTracker) RoundTrip(r *http.Request) (*http.Response, error) {
	object := r.Method == "GET" && strings.Contains(r.URL.Path, "/"+testContainer+"/")
	t.mu.Lock()
	fail := t.fail
	getStatus := t.getStatus
	rangeMode := t.rangeMode
	failHead, badMetadata, badGETLength := t.failHead, t.badMetadata, t.badGETLength
	t.mu.Unlock()
	if r.Method == "HEAD" && failHead {
		return nil, errors.New("injected HEAD failure")
	}
	if object && fail {
		return nil, fmt.Errorf("injected GET failure")
	}
	if object && getStatus != 0 {
		t.mu.Lock()
		t.gets++
		t.active++
		if t.active > t.peak {
			t.peak = t.active
		}
		t.mu.Unlock()
		return &http.Response{
			StatusCode: getStatus, Status: http.StatusText(getStatus), Header: make(http.Header), Request: r,
			Body: &trackedBody{ReadCloser: io.NopCloser(strings.NewReader("injected rejection")), tracker: t},
		}, nil
	}
	request := r
	if object && rangeMode == "ignore" && r.Header.Get("Range") != "" {
		request = r.Clone(r.Context())
		request.Header.Del("Range")
	}
	response, err := (rangeAwareTestTransport{}).RoundTrip(request)
	if err == nil && object && r.Header.Get("Range") != "" {
		switch rangeMode {
		case "missing":
			response.Header.Del("Content-Range")
		case "start":
			response.Header.Set("Content-Range", "bytes 0-5/6")
		case "end":
			response.Header.Set("Content-Range", "bytes 2-4/6")
		case "size":
			response.Header.Set("Content-Range", "bytes 2-5/7")
		case "malformed":
			response.Header.Set("Content-Range", "bytes 2-5/6 junk")
		}
	}
	if err == nil && r.Method == "HEAD" && response.StatusCode < 300 {
		t.mu.Lock()
		t.heads++
		if badMetadata == "etag" {
			response.Header.Del("Etag")
		}
		if badMetadata == "size" {
			response.Header.Set("Content-Length", "-1")
		}
		t.mu.Unlock()
	}
	if err == nil && object && response.StatusCode < 300 {
		t.mu.Lock()

		if badGETLength {
			response.Header.Set("Content-Length", "invalid")
		}
		t.gets++
		t.active++
		if t.active > t.peak {
			t.peak = t.active
		}
		t.ranges = append(t.ranges, r.Header.Get("Range"))
		t.matches = append(t.matches, r.Header.Get("If-Match"))
		t.mu.Unlock()
		response.Body = &trackedBody{ReadCloser: response.Body, tracker: t}
	}
	return response, err
}

func (t *requestTracker) counts() (int, int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.gets, t.active, t.peak
}

func TestSwiftPoolFailureIsolation(t *testing.T) {
	tracker := &requestTracker{}
	conn := setupSwiftServer(t, tracker)
	require.NoError(t, conn.ObjectPutBytes(testContainer, "failure", []byte("abcdef"), ""))
	root, err := NewSwiftFileReader(testContainer, "failure", conn)
	require.NoError(t, err)
	defer func() { require.NoError(t, root.Close()) }()
	clone, err := root.Clone()
	require.NoError(t, err)
	defer func() { require.NoError(t, clone.Close()) }()
	_, err = root.Read(make([]byte, 1))
	require.NoError(t, err)
	tracker.mu.Lock()
	tracker.fail = true
	tracker.mu.Unlock()
	_, err = clone.Read(make([]byte, 1))
	require.ErrorContains(t, err, "injected GET failure")
	data := make([]byte, 1)
	_, err = root.Read(data)
	require.NoError(t, err, "another cursor's open failure must not close the retained stream")
	require.Equal(t, "b", string(data))
	tracker.mu.Lock()
	tracker.fail = false
	tracker.mu.Unlock()
	_, err = clone.Read(data)
	require.NoError(t, err)
	require.Equal(t, "a", string(data))
}

func TestSwiftPoolConcurrentReads(t *testing.T) {
	tracker := &requestTracker{}
	conn := setupSwiftServer(t, tracker)
	data := bytes.Repeat([]byte("abcdefgh"), 4096)
	require.NoError(t, conn.ObjectPutBytes(testContainer, "parallel", data, ""))
	root, err := NewSwiftFileReader(testContainer, "parallel", conn)
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := range 40 {
		c, err := root.Clone()
		require.NoError(t, err)
		wg.Go(func() {
			defer func() { assert.NoError(t, c.Close()) }()
			_, err := c.Seek(int64(i), io.SeekStart)
			if !assert.NoError(t, err) {
				return
			}
			got, err := io.ReadAll(c)
			assert.NoError(t, err)
			assert.Equal(t, data[i:], got)
		})
	}
	wg.Wait()
	require.NoError(t, root.Close())
	_, active, peak := tracker.counts()
	require.Zero(t, active)
	require.LessOrEqual(t, peak, 40)
}

func TestSwiftPoolCleanupErrors(t *testing.T) {
	for _, operation := range []string{"close", "replace", "read", "metadata"} {
		t.Run(operation, func(t *testing.T) {
			tracker := &requestTracker{}
			conn := setupSwiftServer(t, tracker)
			require.NoError(t, conn.ObjectPutBytes(testContainer, "errors", []byte("abcdef"), ""))
			if operation == "metadata" {
				tracker.mu.Lock()
				tracker.failHead = true
				tracker.mu.Unlock()
				_, err := NewSwiftFileReader(testContainer, "errors", conn)
				require.ErrorContains(t, err, "swift object metadata")
			} else {
				root, err := NewSwiftFileReader(testContainer, "errors", conn)
				require.NoError(t, err)
				_, err = root.Read(make([]byte, 1))
				require.NoError(t, err)
				if operation == "read" {
					tracker.mu.Lock()
					tracker.readErr = true
					tracker.mu.Unlock()
				}
				tracker.mu.Lock()
				tracker.closeErr = true
				tracker.mu.Unlock()
				switch operation {
				case "close":
					require.ErrorContains(t, root.Close(), "injected close failure")
				case "replace":
					_, err = root.Seek(2, io.SeekStart)
					require.NoError(t, err)
					_, err = root.Read(make([]byte, 1))
					require.NoError(t, err)
				case "read":
					_, err = root.Read(make([]byte, 1))
					require.ErrorContains(t, err, "injected read failure")
					require.ErrorContains(t, err, "injected close failure")
				}
				tracker.mu.Lock()
				tracker.closeErr = false
				tracker.mu.Unlock()
				require.NoError(t, root.Close())
			}
			_, active, _ := tracker.counts()
			require.Zero(t, active)
		})
	}
}

func TestSwiftOpenSharesPool(t *testing.T) {
	tracker := &requestTracker{}
	conn := setupSwiftServer(t, tracker)
	root, err := func() (source.ParquetFileReader, error) {
		require.NoError(t, conn.ObjectPutBytes(testContainer, "root", []byte("root"), ""))
		return NewSwiftFileReader(testContainer, "root", conn)
	}()
	require.NoError(t, err)
	var cursors []source.ParquetFileReader
	for i := range 20 {
		name := fmt.Sprint(i)
		require.NoError(t, conn.ObjectPutBytes(testContainer, name, []byte(name), ""))
		c, err := root.Open(name)
		require.NoError(t, err)
		cursors = append(cursors, c)
	}
	require.NoError(t, root.Close())
	for i, c := range cursors {
		data, err := io.ReadAll(c)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(i), string(data))
		require.NoError(t, c.Close())
	}
	_, active, peak := tracker.counts()
	require.Zero(t, active)
	require.LessOrEqual(t, peak, 1)
}

func BenchmarkSwiftInterleavedColumns(b *testing.B) {
	for _, width := range []int{4, 32} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			tracker := &requestTracker{}
			conn := setupSwiftServer(b, tracker)
			require.NoError(b, conn.ObjectPutBytes(testContainer, "bench", bytes.Repeat([]byte("x"), 65536), ""))
			buf := make([]byte, 4096)
			b.ResetTimer()
			for range b.N {
				root, err := NewSwiftFileReader(testContainer, "bench", conn)
				require.NoError(b, err)
				cursors := make([]source.ParquetFileReader, width)
				for i := range cursors {
					cursors[i], err = root.Clone()
					require.NoError(b, err)
				}
				require.NoError(b, root.Close())
				for range 8 {
					for _, c := range cursors {
						_, err := io.ReadFull(c, buf)
						require.NoError(b, err)
					}
				}
				for _, c := range cursors {
					require.NoError(b, c.Close())
				}
			}
			b.StopTimer()
			gets, _, peak := tracker.counts()
			b.ReportMetric(float64(gets)/float64(b.N), "GETs/op")
			tracker.mu.Lock()
			heads := tracker.heads
			tracker.mu.Unlock()
			b.ReportMetric(float64(heads)/float64(b.N), "HEADs/op")
			b.ReportMetric(float64(peak), "peak-streams")
		})
	}
}

func TestSwiftOpenOnlyFetchesMetadata(t *testing.T) {
	tracker := &requestTracker{}
	conn := setupSwiftServer(t, tracker)
	require.NoError(t, conn.ObjectPutBytes(testContainer, "metadata", []byte("data"), ""))
	root, err := NewSwiftFileReader(testContainer, "metadata", conn)
	require.NoError(t, err)
	defer func() { require.NoError(t, root.Close()) }()
	gets, active, _ := tracker.counts()
	require.Zero(t, gets)
	require.Zero(t, active)
	tracker.mu.Lock()
	heads := tracker.heads
	tracker.mu.Unlock()
	require.Equal(t, 1, heads)
}

func TestSwiftPoolRejectsChangedObject(t *testing.T) {
	tracker := &requestTracker{}
	conn := setupSwiftServer(t, tracker)
	require.NoError(t, conn.ObjectPutBytes(testContainer, "version", []byte("abcdef"), ""))
	root, err := NewSwiftFileReader(testContainer, "version", conn)
	require.NoError(t, err)
	defer func() { require.NoError(t, root.Close()) }()
	clone, err := root.Clone()
	require.NoError(t, err)
	defer func() { require.NoError(t, clone.Close()) }()
	data := make([]byte, 3)
	_, err = io.ReadFull(root, data)
	require.NoError(t, err)
	require.Equal(t, "abc", string(data))
	_, err = clone.Read(make([]byte, 1))
	require.NoError(t, err)
	require.NoError(t, conn.ObjectPutBytes(testContainer, "version", []byte("UVWXYZ"), ""))
	_, err = root.Seek(2, io.SeekStart)
	require.NoError(t, err)
	n, err := root.Read(data)
	require.Error(t, err, "a resume must not mix bytes from a different object version")
	require.Zero(t, n)
	pos, err := root.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.EqualValues(t, 2, pos)
	tracker.mu.Lock()
	matches := append([]string(nil), tracker.matches...)
	tracker.mu.Unlock()
	require.NotEmpty(t, matches)
	for _, match := range matches {
		require.NotEmpty(t, match, "every GET must be conditional")
		require.Equal(t, matches[0], match, "clones must inherit the original ETag")
	}
	fresh, err := root.Open("version")
	require.NoError(t, err)
	defer func() { require.NoError(t, fresh.Close()) }()
	got, err := io.ReadAll(fresh)
	require.NoError(t, err)
	require.Equal(t, "UVWXYZ", string(got))
}

func TestSwiftRejectsInvalidMetadata(t *testing.T) {
	for _, field := range []string{"etag", "size"} {
		t.Run(field, func(t *testing.T) {
			tracker := &requestTracker{badMetadata: field}
			conn := setupSwiftServer(t, tracker)
			require.NoError(t, conn.ObjectPutBytes(testContainer, "bad-metadata", []byte("data"), ""))
			_, err := NewSwiftFileReader(testContainer, "bad-metadata", conn)
			require.Error(t, err)
		})
	}
}

func TestSwiftPoolClosesFailedOpenBody(t *testing.T) {
	tracker := &requestTracker{badGETLength: true}
	conn := setupSwiftServer(t, tracker)
	require.NoError(t, conn.ObjectPutBytes(testContainer, "bad-get", []byte("abcdef"), ""))
	root, err := NewSwiftFileReader(testContainer, "bad-get", conn)
	require.NoError(t, err)
	defer func() { require.NoError(t, root.Close()) }()
	_, err = root.Read(make([]byte, 1))
	require.Error(t, err)
	_, active, _ := tracker.counts()
	require.Zero(t, active, "an SDK open error may still return a body that needs closing")
}

func TestSwiftPoolRejectsInvalidRange(t *testing.T) {
	for _, mode := range []string{"ignore", "missing", "start", "end", "size", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			tracker := &requestTracker{}
			conn := setupSwiftServer(t, tracker)
			require.NoError(t, conn.ObjectPutBytes(testContainer, "range", []byte("abcdef"), ""))
			root, err := NewSwiftFileReader(testContainer, "range", conn)
			require.NoError(t, err)
			clone, err := root.Clone()
			require.NoError(t, err)
			defer func() { require.NoError(t, root.Close()); require.NoError(t, clone.Close()) }()
			_, err = io.ReadFull(root, make([]byte, 1))
			require.NoError(t, err)
			_, err = clone.Read(make([]byte, 1))
			require.NoError(t, err)
			_, err = root.Seek(2, io.SeekStart)
			require.NoError(t, err)
			tracker.mu.Lock()
			tracker.rangeMode = mode
			tracker.mu.Unlock()
			data := []byte{0xcc, 0xcc}
			n, err := root.Read(data)
			require.ErrorContains(t, err, "Content-Range")
			require.Zero(t, n)
			require.Equal(t, []byte{0xcc, 0xcc}, data, "invalid response bytes must not reach the caller")
			pos, err := root.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			require.EqualValues(t, 2, pos)
			_, active, _ := tracker.counts()
			require.Equal(t, 1, active, "invalid bodies must be closed")
			tracker.mu.Lock()
			tracker.rangeMode = ""
			tracker.mu.Unlock()
			_, err = io.ReadFull(root, data)
			require.NoError(t, err)
			require.Equal(t, "cd", string(data), "a valid retry must resume at the unchanged offset")
			_, _, peak := tracker.counts()
			require.Equal(t, 2, peak)
		})
	}
}

func TestValidContentRange(t *testing.T) {
	tests := []struct {
		name, header   string
		position, size int64
		want           bool
	}{
		{name: "valid", header: "bytes 2-5/6", position: 2, size: 6, want: true},
		{name: "single-byte", header: "bytes 5-5/6", position: 5, size: 6, want: true},
		{name: "leading-zeroes", header: "bytes 02-05/06", position: 2, size: 6, want: true},
		{name: "large", header: "bytes 9223372036854775805-9223372036854775806/9223372036854775807", position: 9223372036854775805, size: 9223372036854775807, want: true},
		{name: "missing", position: 2, size: 6},
		{name: "wrong-unit", header: "items 2-5/6", position: 2, size: 6},
		{name: "no-total", header: "bytes 2-5", position: 2, size: 6},
		{name: "no-end", header: "bytes 2/6", position: 2, size: 6},
		{name: "unknown-total", header: "bytes 2-5/*", position: 2, size: 6},
		{name: "start", header: "bytes 0-5/6", position: 2, size: 6},
		{name: "end", header: "bytes 2-4/6", position: 2, size: 6},
		{name: "total", header: "bytes 2-5/7", position: 2, size: 6},
		{name: "negative", header: "bytes -2-5/6", position: 2, size: 6},
		{name: "signed", header: "bytes +2-5/6", position: 2, size: 6},
		{name: "overflow", header: "bytes 9223372036854775808-5/6", position: 2, size: 6},
		{name: "trailing", header: "bytes 2-5/6 junk", position: 2, size: 6},
		{name: "multiple", header: "bytes 2-5/6, 2-5/6", position: 2, size: 6},
		{name: "invalid-position", header: "bytes 2-5/6", position: -1, size: 6},
		{name: "eof", header: "bytes 6-5/6", position: 6, size: 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, validContentRange(tt.header, tt.position, tt.size)) })
	}
}

func TestSwiftPoolConditionalRequestErrors(t *testing.T) {
	for _, tt := range []struct {
		status  int
		message string
	}{
		{http.StatusPreconditionFailed, "swift object changed while reading \"conditional\""},
		{http.StatusForbidden, "swift object open"},
	} {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			tracker := &requestTracker{}
			conn := setupSwiftServer(t, tracker)
			require.NoError(t, conn.ObjectPutBytes(testContainer, "conditional", []byte("abcdef"), ""))
			root, err := NewSwiftFileReader(testContainer, "conditional", conn)
			require.NoError(t, err)
			defer func() { require.NoError(t, root.Close()) }()
			clone, err := root.Clone()
			require.NoError(t, err)
			defer func() { require.NoError(t, clone.Close()) }()
			_, err = io.ReadFull(root, make([]byte, 1))
			require.NoError(t, err)
			_, err = clone.Read(make([]byte, 1))
			require.NoError(t, err)
			_, err = root.Seek(2, io.SeekStart)
			require.NoError(t, err)
			tracker.mu.Lock()
			tracker.getStatus = tt.status
			tracker.mu.Unlock()
			data := []byte{0xcc, 0xcc}
			n, err := root.Read(data)
			require.ErrorContains(t, err, tt.message)
			var sdkErr *swift.Error
			require.ErrorAs(t, err, &sdkErr)
			require.Equal(t, tt.status, sdkErr.StatusCode)
			require.Zero(t, n)
			require.Equal(t, []byte{0xcc, 0xcc}, data)
			pos, err := root.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			require.EqualValues(t, 2, pos)
			_, active, _ := tracker.counts()
			require.Equal(t, 1, active)
			tracker.mu.Lock()
			tracker.getStatus = 0
			tracker.mu.Unlock()
			_, err = io.ReadFull(root, data)
			require.NoError(t, err)
			require.Equal(t, "cd", string(data))
		})
	}
}
