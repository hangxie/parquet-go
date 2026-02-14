package bloomfilter

import (
	"fmt"

	"github.com/cespare/xxhash/v2"

	"github.com/hangxie/parquet-go/v2/encoding"
	"github.com/hangxie/parquet-go/v2/parquet"
)

// HashValue encodes a value using Parquet plain encoding and hashes it with xxHash.
// For BYTE_ARRAY, the 4-byte length prefix is stripped before hashing (raw bytes only),
// matching the behavior of parquet-java and parquet-cpp.
func HashValue(value any, pT parquet.Type) (uint64, error) {
	encoded, err := encoding.WritePlain([]any{value}, pT)
	if err != nil {
		return 0, fmt.Errorf("encode value for bloom filter hash: %w", err)
	}

	// For BYTE_ARRAY, strip the 4-byte length prefix
	if pT == parquet.Type_BYTE_ARRAY && len(encoded) >= 4 {
		encoded = encoded[4:]
	}

	return xxhash.Sum64(encoded), nil
}
