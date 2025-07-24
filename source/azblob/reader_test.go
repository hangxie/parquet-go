package azblob

import (
	"context"
	"errors"
	"net"
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

func Test_NewAzBlobFileReader(t *testing.T) {
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

func Test_NewAzBlobFileReaderWithClient(t *testing.T) {
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
	expected := "client cannot be nil"
	require.Error(t, err)
	require.Contains(t, err.Error(), expected)
}

func Test_NewAzBlobFileReaderWithSharedKey(t *testing.T) {
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
