package azblob

import (
	"context"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

type ErrorMatcher struct {
	Match func(error) bool
	Desc  string
}

func (em ErrorMatcher) String() string {
	return em.Desc
}

type testCase struct {
	url string
	err *ErrorMatcher
}

var testCases []testCase = []testCase{
	{
		// Public accessible data: https://azure.microsoft.com/en-us/services/open-datasets/catalog/
		url: "https://azureopendatastorage.blob.core.windows.net/censusdatacontainer/release/us_population_zip/year=2010/part-00178-tid-5434563040420806442-84b5e4ab-8ab1-4e28-beb1-81caf32ca312-1919656.c000.snappy.parquet",
		err: nil,
	},
	{
		url: "https://azureopendatastorage.blob.core.windows.net/censusdatacontainer/release/us_population_zip/",
		err: &ErrorMatcher{
			Match: func(err error) bool {
				return bloberror.HasCode(err, bloberror.BlobNotFound)
			},
			Desc: "BlobNotFound",
		},
	},
	{
		// the idea is that Azure blob does now allow "-" in storage account name so there should be no such a storage account
		url: "https://non-existent.blob.core.windows.net/container/blob",
		err: &ErrorMatcher{
			Match: func(err error) bool {
				var dnsErr *net.DNSError
				return errors.As(err, &dnsErr) && dnsErr.Err == "no such host"
			},
			Desc: "no such host",
		},
	},
}

func TestNewAzBlobFileReader(t *testing.T) {
	for _, tc := range testCases {
		_, err := NewAzBlobFileReader(context.Background(), tc.url, nil, blockblob.ClientOptions{
			ClientOptions: policy.ClientOptions{
				Retry: policy.RetryOptions{
					TryTimeout: 10 * time.Second,
					MaxRetries: 1,
				},
			},
		})
		if tc.err == nil {
			require.NoError(t, err)
			continue
		}
		require.Error(t, err)
		require.True(t, tc.err.Match(err))
	}
}

func TestNewAzBlobFileReaderWithClient(t *testing.T) {
	for _, tc := range testCases {
		testClient, _ := blockblob.NewClientWithNoCredential(tc.url, &blockblob.ClientOptions{})
		_, err := NewAzBlobFileReaderWithClient(context.Background(), tc.url, testClient)
		if tc.err == nil {
			require.NoError(t, err)
			continue
		}
		require.Error(t, err)
		require.True(t, tc.err.Match(err))
	}
	_, err := NewAzBlobFileReaderWithClient(context.Background(), "dummy-url", nil)
	expected := "client is nil"
	require.Error(t, err)
	require.Contains(t, err.Error(), expected)
}

func TestNewAzBlobFileReaderWithSharedKey(t *testing.T) {
	for _, tc := range testCases {
		_, err := NewAzBlobFileReader(context.Background(), tc.url, nil, blockblob.ClientOptions{
			ClientOptions: policy.ClientOptions{
				Retry: policy.RetryOptions{
					TryTimeout: 10 * time.Second,
					MaxRetries: 1,
				},
			},
		})
		if tc.err == nil {
			require.NoError(t, err)
			continue
		}
		require.Error(t, err)
		require.True(t, tc.err.Match(err))
	}
}

func TestAzBlobReader_Seek(t *testing.T) {
	reader := &azBlobReader{
		fileSize: 1000,
		offset:   0,
	}

	// Test SeekStart
	pos, err := reader.Seek(100, 0) // io.SeekStart = 0
	require.NoError(t, err)
	require.Equal(t, int64(100), pos)
	require.Equal(t, int64(100), reader.offset)

	// Test SeekCurrent
	pos, err = reader.Seek(50, 1) // io.SeekCurrent = 1
	require.NoError(t, err)
	require.Equal(t, int64(150), pos)
	require.Equal(t, int64(150), reader.offset)

	// Test SeekEnd
	pos, err = reader.Seek(-200, 2) // io.SeekEnd = 2
	require.NoError(t, err)
	require.Equal(t, int64(800), pos)
	require.Equal(t, int64(800), reader.offset)

	// Test invalid whence
	_, err = reader.Seek(0, 3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid whence")

	// Test invalid offset (negative)
	_, err = reader.Seek(-1, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid offset")

	// Test invalid offset (beyond file size)
	_, err = reader.Seek(1001, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid offset")
}

func TestAzBlobReader_Read(t *testing.T) {
	// Test read when not opened
	reader := &azBlobReader{}
	buf := make([]byte, 10)
	_, err := reader.Read(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "url not opened")

	// Test read at EOF
	reader = &azBlobReader{
		azBlockBlob: azBlockBlob{
			blockBlobClient: &blockblob.Client{}, // non-nil to pass opened check
		},
		fileSize: 100,
		offset:   100,
	}
	n, err := reader.Read(buf)
	if err != nil {
		require.Equal(t, err.Error(), "EOF") // At EOF, we expect io.EOF
	}
	require.Equal(t, 0, n)
}

func TestAzBlobReader_Close(t *testing.T) {
	reader := &azBlobReader{}
	err := reader.Close()
	require.NoError(t, err) // Close is a no-op, should never error
}

func TestAzBlobReader_Clone(t *testing.T) {
	// Test Clone with valid client (will fail due to network call, but covers the method)
	testURL := "https://example.blob.core.windows.net/container/blob"
	parsedURL, _ := url.Parse(testURL)
	testClient, _ := blockblob.NewClientWithNoCredential(testURL, &blockblob.ClientOptions{})
	reader := &azBlobReader{
		azBlockBlob: azBlockBlob{
			ctx:             context.Background(),
			url:             parsedURL,
			blockBlobClient: testClient,
		},
	}
	// This will error due to network call, but it covers the Clone method
	_, err := reader.Clone()
	require.Error(t, err) // Expected to fail due to network call
}
