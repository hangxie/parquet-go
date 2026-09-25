package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestApplyFixedTypeLength(t *testing.T) {
	testCases := map[string]struct {
		schema   parquet.SchemaElement
		info     Tag
		expected *int32
	}{
		"uuid-on-fixed-len": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			},
			Tag{},
			ToPtr(int32(16)),
		},
		// A UUID on the wrong physical type is rejected by validation, but the width
		// must not be stamped onto the element on the way there.
		"uuid-on-byte-array": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			},
			Tag{},
			nil,
		},
		"float16-on-byte-array": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			},
			Tag{},
			nil,
		},
		"interval-on-byte-array": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: ToPtr(parquet.ConvertedType_INTERVAL),
			},
			Tag{},
			nil,
		},
		"nil-type": {
			parquet.SchemaElement{
				LogicalType: &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			},
			Tag{},
			nil,
		},
		"unannotated": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)},
			Tag{},
			nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			applyFixedTypeLength(&tc.schema, &tc.info)
			if tc.expected == nil {
				require.Nil(t, tc.schema.TypeLength)
				return
			}
			require.NotNil(t, tc.schema.TypeLength)
			require.Equal(t, *tc.expected, *tc.schema.TypeLength)
		})
	}
}
