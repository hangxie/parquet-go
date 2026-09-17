package types

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// errNoPhysicalType reports a schema element with no physical type to scan into.
func errNoPhysicalType(s string) error {
	return fmt.Errorf("cannot scan %q without a physical type", s)
}

// isTextAnnotated reports whether either annotation guarantees the column holds text,
// which is verbatim in both modes; base64-decoding it is the corruption in #420.
func isTextAnnotated(cT *parquet.ConvertedType, lT *parquet.LogicalType) bool {
	if lT != nil && (lT.IsSetSTRING() || lT.IsSetENUM() || lT.IsSetJSON()) {
		return true
	}
	if cT == nil {
		return false
	}
	switch *cT {
	case parquet.ConvertedType_UTF8, parquet.ConvertedType_ENUM, parquet.ConvertedType_JSON:
		return true
	}
	return false
}
