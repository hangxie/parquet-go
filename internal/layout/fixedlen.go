package layout

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// fixedLenByteArrayWidth returns the width of a FIXED_LEN_BYTE_ARRAY or INT96 column, false for any other.
func fixedLenByteArrayWidth(schema *parquet.SchemaElement) (int, bool) {
	if schema == nil || schema.Type == nil {
		return 0, false
	}
	switch *schema.Type {
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return int(schema.GetTypeLength()), true
	case parquet.Type_INT96:
		return 12, true
	}
	return 0, false
}

// checkWidth rejects a value that is not as wide as its column.
func checkWidth(schema *parquet.SchemaElement, path []string, width int, val any) error {
	// Values that carry no bytes are left to the encoder, which reports the type
	// mismatch itself.
	var ln int
	switch value := val.(type) {
	case string:
		ln = len(value)
	case []byte:
		ln = len(value)
	default:
		return nil
	}
	if ln == width {
		return nil
	}
	return fmt.Errorf("column [%s]: %s value of length %d does not match column length %d", strings.Join(path, "."), schema.GetType(), ln, width)
}

// checkFixedLenByteArrayWidth rejects one value that is not as wide as its column.
func checkFixedLenByteArrayWidth(schema *parquet.SchemaElement, path []string, val any) error {
	// Nothing downstream catches this: a wider value is truncated when the file is
	// read back and a narrower one leaves the column chunk unreadable, in both cases
	// without an error at write time.
	width, ok := fixedLenByteArrayWidth(schema)
	if !ok {
		return nil
	}
	return checkWidth(schema, path, width, val)
}

// checkFixedLenByteArrayWidths applies checkFixedLenByteArrayWidth to a page of values.
func checkFixedLenByteArrayWidths(schema *parquet.SchemaElement, path []string, values []any) error {
	width, ok := fixedLenByteArrayWidth(schema)
	if !ok {
		return nil
	}
	for i := range values {
		if err := checkWidth(schema, path, width, values[i]); err != nil {
			return err
		}
	}
	return nil
}
