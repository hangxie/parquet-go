package http

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v3/source"
)

// Compile time check that *httpReader implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*httpReader)(nil)

type httpReader struct {
	url            string
	size           int64
	offset         int64
	httpClient     *http.Client
	extraHeaders   map[string]string
	ignoreTLSError bool

	dedicatedTransport bool
}

const (
	rangeHeader        = "Range"
	rangeFormat        = "bytes=%d-%d"
	contentRangeHeader = "Content-Range"
)

func NewHttpReader(uri string, dedicatedTransport, ignoreTLSError bool, extraHeaders map[string]string) (source.ParquetFileReader, error) {
	// make sure remote support range
	var transport *http.Transport
	if dedicatedTransport {
		transport = &http.Transport{}
	} else {
		// Clone the default transport to avoid race conditions
		defaultTransport := http.DefaultTransport.(*http.Transport)
		transport = defaultTransport.Clone()
	}
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: ignoreTLSError}
	client := &http.Client{Transport: transport}

	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range extraHeaders {
		req.Header.Add(k, v)
	}
	req.Header.Add(rangeHeader, fmt.Sprintf(rangeFormat, 0, 0))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// retrieve size
	contentRange := resp.Header.Values(contentRangeHeader)
	if len(contentRange) == 0 {
		return nil, fmt.Errorf("remote [%s] does not support range", uri)
	}

	tmp := strings.Split(contentRange[0], "/")
	if len(tmp) != 2 {
		return nil, fmt.Errorf("%s format is unknown: %s", contentRangeHeader, contentRange[0])
	}

	size, err := strconv.ParseInt(tmp[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse data size from %s: %s", contentRangeHeader, contentRange[0])
	}

	return &httpReader{
		url:                uri,
		size:               size,
		offset:             0,
		httpClient:         client,
		extraHeaders:       extraHeaders,
		ignoreTLSError:     ignoreTLSError,
		dedicatedTransport: dedicatedTransport,
	}, nil
}

// NewHttpReaderWithClient creates an HTTP-based parquet file reader using the
// provided *http.Client. The caller owns the client lifecycle — this function
// does not close the client or its transport when the reader is closed.
// Callers must configure timeouts on the provided client (e.g., http.Client.Timeout
// or per-request context deadlines) and may need to call
// Transport.CloseIdleConnections() when done to release resources.
//
// Example with custom timeouts:
//
//	client := &http.Client{
//	    Timeout: 30 * time.Second,
//	    Transport: &http.Transport{
//	        MaxIdleConns:          100,
//	        IdleConnTimeout:       90 * time.Second,
//	        TLSHandshakeTimeout:   10 * time.Second,
//	        ExpectContinueTimeout: 1 * time.Second,
//	    },
//	}
//	reader, err := NewHttpReaderWithClient(uri, client, headers)
//
// Skip TLS verification (e.g., for AWS S3 wildcard cert issues):
//
//	client := &http.Client{
//	    Transport: &http.Transport{
//	        TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
//	    },
//	}
func NewHttpReaderWithClient(uri string, client *http.Client, extraHeaders map[string]string) (source.ParquetFileReader, error) {
	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range extraHeaders {
		req.Header.Add(k, v)
	}
	req.Header.Add(rangeHeader, fmt.Sprintf(rangeFormat, 0, 0))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	contentRange := resp.Header.Values(contentRangeHeader)
	if len(contentRange) == 0 {
		return nil, fmt.Errorf("remote [%s] does not support range", uri)
	}
	tmp := strings.Split(contentRange[0], "/")
	if len(tmp) != 2 {
		return nil, fmt.Errorf("%s format is unknown: %s", contentRangeHeader, contentRange[0])
	}
	size, err := strconv.ParseInt(tmp[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse data size from %s: %s", contentRangeHeader, contentRange[0])
	}

	// Extract ignoreTLSError from the client's transport if possible.
	var ignoreTLS bool
	if t, ok := client.Transport.(*http.Transport); ok && t.TLSClientConfig != nil {
		ignoreTLS = t.TLSClientConfig.InsecureSkipVerify
	}

	return &httpReader{
		url:            uri,
		size:           size,
		offset:         0,
		httpClient:     client,
		extraHeaders:   extraHeaders,
		ignoreTLSError: ignoreTLS,
	}, nil
}

func (r *httpReader) Open(_ string) (source.ParquetFileReader, error) {
	return NewHttpReaderWithClient(r.url, r.httpClient, r.extraHeaders)
}

func (r httpReader) Clone() (source.ParquetFileReader, error) {
	// Create a new instance without making an HTTP request
	// since we already have all the necessary information
	return &httpReader{
		url:                r.url,
		size:               r.size,
		offset:             0,
		httpClient:         r.httpClient,
		extraHeaders:       r.extraHeaders,
		ignoreTLSError:     r.ignoreTLSError,
		dedicatedTransport: r.dedicatedTransport,
	}, nil
}

func (r *httpReader) Seek(offset int64, pos int) (int64, error) {
	switch pos {
	case io.SeekStart:
		r.offset = offset
	case io.SeekCurrent:
		r.offset += offset
	case io.SeekEnd:
		r.offset = r.size + offset
	default:
		return 0, fmt.Errorf("unknown whence: %d", pos)
	}

	if r.offset < 0 {
		r.offset = 0
	} else if r.offset >= r.size {
		r.offset = r.size
	}

	return r.offset, nil
}

func (r *httpReader) Read(b []byte) (int, error) {
	req, err := http.NewRequest(http.MethodGet, r.url, nil)
	if err != nil {
		return 0, err
	}

	for k, v := range r.extraHeaders {
		req.Header.Add(k, v)
	}
	req.Header.Add(rangeHeader, fmt.Sprintf(rangeFormat, r.offset, r.offset+int64(len(b)-1)))
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	buf, err := io.ReadAll(resp.Body)
	bytesRead := len(buf)
	copy(b, buf)

	r.offset += int64(bytesRead)
	if r.offset > r.size {
		r.offset = r.size
	}
	return bytesRead, err
}

func (r *httpReader) Close() error {
	return nil
}
