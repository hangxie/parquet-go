package writer

import (
	"fmt"
	"reflect"
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

// validateOptionalFields rejects OPTIONAL struct fields that cannot hold a null.
func validateOptionalFields(t reflect.Type, path []string) error {
	// Marshal only raises the definition level through a pointer, or an interface on
	// a VARIANT column, so any other OPTIONAL field is written as null whatever it
	// holds. Readers fill nulls with zero values, so only writers check.
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return validateOptionalFields(t.Elem(), path)
	case reflect.Struct:
	default:
		return nil
	}
	for i := range t.NumField() {
		f := t.Field(i)
		tagStr := f.Tag.Get("parquet")
		if tagStr == "" {
			continue
		}
		tag, err := common.StringToTag(tagStr)
		if err != nil {
			return fmt.Errorf("parse tag for field %s: %w", f.Name, err)
		}
		fieldPath := append(path[:len(path):len(path)], tag.ExName)
		if tag.RepetitionType == parquet.FieldRepetitionType_OPTIONAL &&
			f.Type.Kind() != reflect.Pointer &&
			(f.Type.Kind() != reflect.Interface || tag.Type != "VARIANT") {
			return fmt.Errorf("field [%s]: OPTIONAL requires a pointer, got %s",
				strings.Join(fieldPath, "."), f.Type)
		}
		if err := validateOptionalFields(f.Type, fieldPath); err != nil {
			return err
		}
	}
	return nil
}
