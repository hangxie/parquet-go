package writer

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// validateSchemaForWrite rejects columns no value can be written into.
func (pw *ParquetWriter) validateSchemaForWrite() error {
	// This covers the schema inputs that reach a writer without passing through tag
	// validation: a raw []*parquet.SchemaElement or a *schema.SchemaHandler. Schemas
	// built from struct tags, CSV metadata, a JSON schema, or an arrow schema are
	// checked as each element is built, so those writers need no call of their own.
	if pw.SchemaHandler == nil {
		return nil
	}
	if err := pw.SchemaHandler.ValidateUniqueNames(); err != nil {
		return err
	}
	if err := pw.validateDictionaryEncodings(); err != nil {
		return err
	}
	for idx, se := range pw.SchemaHandler.SchemaElements {
		if se == nil || se.Type == nil || *se.Type != parquet.Type_FIXED_LEN_BYTE_ARRAY {
			continue
		}
		// A zero-width column accepts no value at all: everything written to it
		// comes back empty, which makes the declaration a mistake rather than a
		// usable column.
		if se.GetTypeLength() <= 0 {
			return fmt.Errorf("column [%s]: FIXED_LEN_BYTE_ARRAY requires a positive length, got %d",
				pw.columnPath(idx), se.GetTypeLength())
		}
	}
	return nil
}

// columnPath names a schema element for an error message, preferring its full path.
func (pw *ParquetWriter) columnPath(idx int) string {
	if path, ok := pw.SchemaHandler.IndexMap[int32(idx)]; ok {
		return strings.Join(common.StrToPath(path), ".")
	}
	return pw.SchemaHandler.SchemaElements[idx].Name
}

// validateDictionaryEncodings rejects BOOLEAN dictionary columns, which Arrow and other readers cannot decode.
func (pw *ParquetWriter) validateDictionaryEncodings() error {
	// Readers still accept the tag so files written before this check stay readable.
	for idx, se := range pw.SchemaHandler.SchemaElements {
		if se == nil || se.Type == nil || *se.Type != parquet.Type_BOOLEAN || idx >= len(pw.SchemaHandler.Infos) {
			continue
		}
		info := pw.SchemaHandler.Infos[idx]
		if info == nil {
			continue
		}
		if info.Encoding == parquet.Encoding_PLAIN_DICTIONARY || info.Encoding == parquet.Encoding_RLE_DICTIONARY {
			return fmt.Errorf("column [%s]: dictionary encoding is not supported for BOOLEAN", pw.columnPath(idx))
		}
	}
	return nil
}
