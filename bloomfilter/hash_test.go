package bloomfilter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func TestHashValue(t *testing.T) {
	testCases := map[string]struct {
		value  any
		pType  parquet.Type
		errMsg string
	}{
		"int32":                {int32(42), parquet.Type_INT32, ""},
		"int64":                {int64(42), parquet.Type_INT64, ""},
		"float32":              {float32(3.14), parquet.Type_FLOAT, ""},
		"float64":              {float64(3.14), parquet.Type_DOUBLE, ""},
		"bool-true":            {true, parquet.Type_BOOLEAN, ""},
		"bool-false":           {false, parquet.Type_BOOLEAN, ""},
		"byte-array":           {"hello", parquet.Type_BYTE_ARRAY, ""},
		"byte-array-empty":     {"", parquet.Type_BYTE_ARRAY, ""},
		"fixed-len-byte-array": {"abcdefgh", parquet.Type_FIXED_LEN_BYTE_ARRAY, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hash, err := HashValue(tc.value, tc.pType)
			if tc.errMsg == "" {
				require.NoError(t, err)
				require.NotEqual(t, uint64(0), hash)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func TestHashValueEncodingError(t *testing.T) {
	// Passing a string value with INT32 type causes WritePlain to fail
	_, err := HashValue("not-an-int", parquet.Type_INT32)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode value for bloom filter hash")
}

func TestHashValueDeterministic(t *testing.T) {
	// Same value should always produce the same hash
	h1, err := HashValue(int64(12345), parquet.Type_INT64)
	require.NoError(t, err)
	h2, err := HashValue(int64(12345), parquet.Type_INT64)
	require.NoError(t, err)
	require.Equal(t, h1, h2)
}

func TestHashValueDifferentValues(t *testing.T) {
	// Different values should (very likely) produce different hashes
	h1, err := HashValue(int64(1), parquet.Type_INT64)
	require.NoError(t, err)
	h2, err := HashValue(int64(2), parquet.Type_INT64)
	require.NoError(t, err)
	require.NotEqual(t, h1, h2)
}

func TestHashValueByteArrayStripsPrefix(t *testing.T) {
	// BYTE_ARRAY should hash raw bytes, not the length-prefixed encoding
	// Two identical strings should produce the same hash
	h1, err := HashValue("test", parquet.Type_BYTE_ARRAY)
	require.NoError(t, err)
	h2, err := HashValue("test", parquet.Type_BYTE_ARRAY)
	require.NoError(t, err)
	require.Equal(t, h1, h2)

	// BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY for the same string should produce the same hash
	// (since we strip the 4-byte prefix for BYTE_ARRAY)
	h3, err := HashValue("test", parquet.Type_FIXED_LEN_BYTE_ARRAY)
	require.NoError(t, err)
	require.Equal(t, h1, h3)
}

func TestHashValueRoundTrip(t *testing.T) {
	// Test that hashed values can be found in a bloom filter
	f := New(1024)

	values := []struct {
		val   any
		pType parquet.Type
	}{
		{int32(42), parquet.Type_INT32},
		{int64(12345), parquet.Type_INT64},
		{float32(3.14), parquet.Type_FLOAT},
		{float64(2.718), parquet.Type_DOUBLE},
		{"hello world", parquet.Type_BYTE_ARRAY},
		{true, parquet.Type_BOOLEAN},
	}

	// Insert all values
	for _, v := range values {
		h, err := HashValue(v.val, v.pType)
		require.NoError(t, err)
		f.Insert(h)
	}

	// Check all values
	for _, v := range values {
		h, err := HashValue(v.val, v.pType)
		require.NoError(t, err)
		require.True(t, f.Check(h))
	}
}
