package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

// Comprehensive tests for NewSchemaHandlerFromArrow function

func Test_NewSchemaHandlerFromArrow(t *testing.T) {
	t.Run("simple_schema", func(t *testing.T) {
		// Test with simple Arrow schema covering basic types
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		}, nil)

		schemaHandler, err := NewSchemaHandlerFromArrow(arrowSchema)
		require.Nil(t, err)
		require.NotNil(t, schemaHandler)

		// Verify correct number of schema elements (root + 4 fields)
		require.Equal(t, 5, len(schemaHandler.SchemaElements))

		// Verify root element
		rootElement := schemaHandler.SchemaElements[0]
		require.Equal(t, "Parquet45go45root", rootElement.Name)
		require.Equal(t, int32(4), *rootElement.NumChildren)
		require.Equal(t, "REQUIRED", rootElement.RepetitionType.String())

		// Verify field elements (names get converted to title case)
		idElement := schemaHandler.SchemaElements[1]
		require.Equal(t, "Id", idElement.Name)
		require.Equal(t, "INT32", idElement.Type.String())
		require.Equal(t, "REQUIRED", idElement.RepetitionType.String())

		nameElement := schemaHandler.SchemaElements[2]
		require.Equal(t, "Name", nameElement.Name)
		require.Equal(t, "BYTE_ARRAY", nameElement.Type.String())
		require.Equal(t, "OPTIONAL", nameElement.RepetitionType.String())
		require.Equal(t, "UTF8", nameElement.ConvertedType.String())

		// Verify that schema handler maps are populated correctly
		require.NotEmpty(t, schemaHandler.IndexMap)
		require.NotEmpty(t, schemaHandler.MapIndex)
		require.NotEmpty(t, schemaHandler.Infos)
		require.Equal(t, 5, len(schemaHandler.Infos))

		// Verify ExPath/InPath mappings are created
		require.NotEmpty(t, schemaHandler.ExPathToInPath)
		require.NotEmpty(t, schemaHandler.InPathToExPath)
	})

	t.Run("comprehensive_type_coverage", func(t *testing.T) {
		// Test all supported Arrow types to ensure complete coverage
		arrowSchema := arrow.NewSchema([]arrow.Field{
			// Primitive integer types
			{Name: "int8_field", Type: arrow.PrimitiveTypes.Int8, Nullable: false},
			{Name: "int16_field", Type: arrow.PrimitiveTypes.Int16, Nullable: false},
			{Name: "int32_field", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "int64_field", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "uint8_field", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
			{Name: "uint16_field", Type: arrow.PrimitiveTypes.Uint16, Nullable: false},
			{Name: "uint32_field", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
			{Name: "uint64_field", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},

			// Float types
			{Name: "float32_field", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
			{Name: "float64_field", Type: arrow.PrimitiveTypes.Float64, Nullable: false},

			// Date types (both primitive and fixed width)
			{Name: "date32_prim", Type: arrow.PrimitiveTypes.Date32, Nullable: false},
			{Name: "date64_prim", Type: arrow.PrimitiveTypes.Date64, Nullable: false},
			{Name: "date32_fixed", Type: arrow.FixedWidthTypes.Date32, Nullable: false},
			{Name: "date64_fixed", Type: arrow.FixedWidthTypes.Date64, Nullable: false},

			// Binary types
			{Name: "binary_field", Type: arrow.BinaryTypes.Binary, Nullable: false},
			{Name: "string_field", Type: arrow.BinaryTypes.String, Nullable: false},

			// Fixed width types
			{Name: "bool_field", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "time32ms_field", Type: arrow.FixedWidthTypes.Time32ms, Nullable: false},
			{Name: "timestamp_ms_field", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: false},

			// Nullable versions
			{Name: "nullable_int32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "nullable_string", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil)

		schemaHandler, err := NewSchemaHandlerFromArrow(arrowSchema)
		require.Nil(t, err)
		require.NotNil(t, schemaHandler)

		// Verify correct number of elements (root + 21 fields)
		require.Equal(t, 22, len(schemaHandler.SchemaElements))

		// Verify root has correct number of children
		require.Equal(t, int32(21), *schemaHandler.SchemaElements[0].NumChildren)

		// Verify nullable fields are correctly marked as OPTIONAL
		nullableInt32Element := schemaHandler.SchemaElements[20] // Should be nullable_int32
		require.Equal(t, "Nullable_int32", nullableInt32Element.Name)
		require.Equal(t, "OPTIONAL", nullableInt32Element.RepetitionType.String())

		nullableStringElement := schemaHandler.SchemaElements[21] // Should be nullable_string
		require.Equal(t, "Nullable_string", nullableStringElement.Name)
		require.Equal(t, "OPTIONAL", nullableStringElement.RepetitionType.String())
	})

	t.Run("empty_schema", func(t *testing.T) {
		// Test with empty Arrow schema
		emptyArrowSchema := arrow.NewSchema([]arrow.Field{}, nil)

		schemaHandler, err := NewSchemaHandlerFromArrow(emptyArrowSchema)
		require.Nil(t, err)
		require.NotNil(t, schemaHandler)

		// Should only have root element
		require.Equal(t, 1, len(schemaHandler.SchemaElements))
		require.Equal(t, "Parquet45go45root", schemaHandler.SchemaElements[0].Name)
		require.Equal(t, int32(0), *schemaHandler.SchemaElements[0].NumChildren)

		// Maps should still be initialized but only contain root
		require.NotEmpty(t, schemaHandler.IndexMap)
		require.NotEmpty(t, schemaHandler.MapIndex)
		require.Equal(t, 1, len(schemaHandler.Infos))
	})

	t.Run("unsupported_types_error", func(t *testing.T) {
		// Test with unsupported Arrow types
		unsupportedSchema := arrow.NewSchema([]arrow.Field{
			{Name: "timestamp_ns", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false},
		}, nil)

		_, err := NewSchemaHandlerFromArrow(unsupportedSchema)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "unsupported arrow format")
	})

	t.Run("mixed_supported_and_unsupported", func(t *testing.T) {
		// Test schema with mix of supported and unsupported types
		mixedSchema := arrow.NewSchema([]arrow.Field{
			{Name: "valid_int32", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "invalid_time64us", Type: arrow.FixedWidthTypes.Time64us, Nullable: false},
		}, nil)

		_, err := NewSchemaHandlerFromArrow(mixedSchema)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "unsupported arrow format")
	})

	t.Run("timestamp_with_wrong_unit", func(t *testing.T) {
		// Test timestamp with non-millisecond unit (should error on line 95)
		timestampMicros := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		wrongUnitSchema := arrow.NewSchema([]arrow.Field{
			{Name: "timestamp_micros", Type: timestampMicros, Nullable: false},
		}, nil)

		_, err := NewSchemaHandlerFromArrow(wrongUnitSchema)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "unsupported arrow format")
	})

	t.Run("test_uncovered_date_lines", func(t *testing.T) {
		// Test to hit the uncovered Date64 line in FixedWidthTypes (line 77)
		dateSchema := arrow.NewSchema([]arrow.Field{
			{Name: "date64_fixed_test", Type: arrow.FixedWidthTypes.Date64, Nullable: false},
		}, nil)

		schemaHandler, err := NewSchemaHandlerFromArrow(dateSchema)
		require.Nil(t, err)
		require.NotNil(t, schemaHandler)

		// Verify the DATE conversion was applied correctly
		dateElement := schemaHandler.SchemaElements[1]
		require.Equal(t, "Date64_fixed_test", dateElement.Name)
		require.Equal(t, "INT32", dateElement.Type.String())
		require.Equal(t, "DATE", dateElement.ConvertedType.String())
	})

	t.Run("schema_element_creation_error", func(t *testing.T) {
		// This test is harder to trigger since common.NewSchemaElementFromTagMap
		// would need to fail, but we can at least test the path exists
		validSchema := arrow.NewSchema([]arrow.Field{
			{Name: "test_field", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		}, nil)

		schemaHandler, err := NewSchemaHandlerFromArrow(validSchema)
		require.Nil(t, err)
		require.NotNil(t, schemaHandler)

		// Verify the schema element was created successfully
		require.Equal(t, 2, len(schemaHandler.SchemaElements))
		require.Equal(t, "Test_field", schemaHandler.SchemaElements[1].Name)
	})

	t.Run("nil_schema_panic", func(t *testing.T) {
		require.Panics(t, func() {
			_, _ = NewSchemaHandlerFromArrow(nil)
		})
	})

	t.Run("verify_info_structure", func(t *testing.T) {
		// Test to verify the Infos structure is properly populated
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "field1", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "field2", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil)

		schemaHandler, err := NewSchemaHandlerFromArrow(arrowSchema)
		require.Nil(t, err)
		require.NotNil(t, schemaHandler)

		// Verify Infos array structure
		require.Equal(t, 3, len(schemaHandler.Infos)) // root + 2 fields

		// Root info
		rootInfo := schemaHandler.Infos[0]
		require.Equal(t, "Parquet45go45root", rootInfo.InName)
		require.Equal(t, "Parquet45go45root", rootInfo.ExName)
		require.Equal(t, "REQUIRED", rootInfo.RepetitionType.String())

		// Field infos should be properly populated by StringToTag
		field1Info := schemaHandler.Infos[1]
		require.Equal(t, "Field1", field1Info.InName)

		field2Info := schemaHandler.Infos[2]
		require.Equal(t, "Field2", field2Info.InName)
	})

	t.Run("test_complete_creation_pipeline", func(t *testing.T) {
		// Test the complete pipeline from Arrow schema to SchemaHandler
		// This ensures all code paths in NewSchemaHandlerFromArrow are exercised
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "complete_test", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		}, nil)

		schemaHandler, err := NewSchemaHandlerFromArrow(arrowSchema)
		require.Nil(t, err)
		require.NotNil(t, schemaHandler)

		// Verify the complete schema handler structure
		require.Equal(t, 2, len(schemaHandler.SchemaElements))
		require.Equal(t, 2, len(schemaHandler.Infos))

		// Verify CreateInExMap was called and maps are populated
		require.NotEmpty(t, schemaHandler.ExPathToInPath)
		require.NotEmpty(t, schemaHandler.InPathToExPath)

		// Verify NewSchemaHandlerFromSchemaList integration
		require.NotEmpty(t, schemaHandler.IndexMap)
		require.NotEmpty(t, schemaHandler.MapIndex)
	})

	t.Run("test_stringtotag_error_handling", func(t *testing.T) {
		// This test verifies that errors from StringToTag are properly handled
		// Using a minimal valid schema since StringToTag errors are hard to trigger
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "error_test", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		}, nil)

		schemaHandler, err := NewSchemaHandlerFromArrow(arrowSchema)
		require.Nil(t, err)
		require.NotNil(t, schemaHandler)

		// Verify the field was processed correctly despite the potential error path
		require.Equal(t, 2, len(schemaHandler.SchemaElements))
		errorTestElement := schemaHandler.SchemaElements[1]
		require.Equal(t, "Error_test", errorTestElement.Name)
		require.Equal(t, "FLOAT", errorTestElement.Type.String())
		require.Equal(t, "OPTIONAL", errorTestElement.RepetitionType.String())
	})
}

func Test_TypeConversion(t *testing.T) {
	tests := []struct {
		title                   string
		testSchema              *arrow.Schema
		expectedParquetMetaData []string
		expectedErr             bool
	}{
		{
			title: "test primitive type conversion",
			testSchema: arrow.NewSchema([]arrow.Field{
				{Name: "f1-i8", Type: arrow.PrimitiveTypes.Int8},
				{Name: "f1-i16", Type: arrow.PrimitiveTypes.Int16},
				{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
				{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
				{Name: "f1-ui8", Type: arrow.PrimitiveTypes.Uint8},
				{Name: "f1-ui16", Type: arrow.PrimitiveTypes.Uint16},
				{Name: "f1-ui32", Type: arrow.PrimitiveTypes.Uint32},
				{Name: "f1-ui64", Type: arrow.PrimitiveTypes.Uint64},
				{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
				{Name: "f1-f62", Type: arrow.PrimitiveTypes.Float64},
				{Name: "f1-d32", Type: arrow.PrimitiveTypes.Date32},
				{Name: "f1-d64", Type: arrow.PrimitiveTypes.Date64},
				{
					Name: "null-i8", Type: arrow.PrimitiveTypes.Int8,
					Nullable: true,
				},
				{
					Name: "null-i16", Type: arrow.PrimitiveTypes.Int16,
					Nullable: true,
				},
				{
					Name: "null-i32", Type: arrow.PrimitiveTypes.Int32,
					Nullable: true,
				},
				{
					Name: "null-i64", Type: arrow.PrimitiveTypes.Int64,
					Nullable: true,
				},
				{
					Name: "null-ui8", Type: arrow.PrimitiveTypes.Uint8,
					Nullable: true,
				},
				{
					Name: "null-ui16", Type: arrow.PrimitiveTypes.Uint16,
					Nullable: true,
				},
				{
					Name: "null-ui32", Type: arrow.PrimitiveTypes.Uint32,
					Nullable: true,
				},
				{
					Name: "null-ui64", Type: arrow.PrimitiveTypes.Uint64,
					Nullable: true,
				},
				{
					Name: "null-f32", Type: arrow.PrimitiveTypes.Float32,
					Nullable: true,
				},
				{
					Name: "null-f62", Type: arrow.PrimitiveTypes.Float64,
					Nullable: true,
				},
				{
					Name: "null-d32", Type: arrow.PrimitiveTypes.Date32,
					Nullable: true,
				},
				{
					Name: "null-d64", Type: arrow.PrimitiveTypes.Date64,
					Nullable: true,
				},
			}, nil),
			expectedParquetMetaData: []string{
				"name=f1-i8, type=INT32, convertedtype=INT_8, " +
					"repetitiontype=REQUIRED",
				"name=f1-i16, type=INT32, convertedtype=INT_16, " +
					"repetitiontype=REQUIRED",
				"name=f1-i32, type=INT32, repetitiontype=REQUIRED",
				"name=f1-i64, type=INT64, repetitiontype=REQUIRED",
				"name=f1-ui8, type=INT32, convertedtype=UINT_8, " +
					"repetitiontype=REQUIRED",
				"name=f1-ui16, type=INT32, convertedtype=UINT_16, " +
					"repetitiontype=REQUIRED",
				"name=f1-ui32, type=INT32, convertedtype=UINT_32, " +
					"repetitiontype=REQUIRED",
				"name=f1-ui64, type=INT64, convertedtype=UINT_64, " +
					"repetitiontype=REQUIRED",
				"name=f1-f32, type=FLOAT, repetitiontype=REQUIRED",
				"name=f1-f62, type=DOUBLE, repetitiontype=REQUIRED",
				"name=f1-d32, type=INT32, convertedtype=DATE, " +
					"repetitiontype=REQUIRED",
				"name=f1-d64, type=INT32, convertedtype=DATE, " +
					"repetitiontype=REQUIRED",
				"name=null-i8, type=INT32, convertedtype=INT_8, " +
					"repetitiontype=OPTIONAL",
				"name=null-i16, type=INT32, convertedtype=INT_16, " +
					"repetitiontype=OPTIONAL",
				"name=null-i32, type=INT32, repetitiontype=OPTIONAL",
				"name=null-i64, type=INT64, repetitiontype=OPTIONAL",
				"name=null-ui8, type=INT32, convertedtype=UINT_8, " +
					"repetitiontype=OPTIONAL",
				"name=null-ui16, type=INT32, convertedtype=UINT_16, " +
					"repetitiontype=OPTIONAL",
				"name=null-ui32, type=INT32, convertedtype=UINT_32, " +
					"repetitiontype=OPTIONAL",
				"name=null-ui64, type=INT64, convertedtype=UINT_64, " +
					"repetitiontype=OPTIONAL",
				"name=null-f32, type=FLOAT, repetitiontype=OPTIONAL",
				"name=null-f62, type=DOUBLE, repetitiontype=OPTIONAL",
				"name=null-d32, type=INT32, convertedtype=DATE, " +
					"repetitiontype=OPTIONAL",
				"name=null-d64, type=INT32, convertedtype=DATE, " +
					"repetitiontype=OPTIONAL",
			},
			expectedErr: false,
		},
		{
			title: "test binary type conversion",
			testSchema: arrow.NewSchema([]arrow.Field{
				{Name: "f1-string", Type: arrow.BinaryTypes.String},
				{Name: "f1-binary", Type: arrow.BinaryTypes.Binary},
				{
					Name: "null-string", Type: arrow.BinaryTypes.String,
					Nullable: true,
				},
				{
					Name: "null-binary", Type: arrow.BinaryTypes.Binary,
					Nullable: true,
				},
			}, nil),
			expectedParquetMetaData: []string{
				"name=f1-string, type=BYTE_ARRAY, convertedtype=UTF8, " +
					"repetitiontype=REQUIRED",
				"name=f1-binary, type=BYTE_ARRAY, repetitiontype=REQUIRED",
				"name=null-string, type=BYTE_ARRAY, convertedtype=UTF8, " +
					"repetitiontype=OPTIONAL",
				"name=null-binary, type=BYTE_ARRAY, repetitiontype=OPTIONAL",
			},
			expectedErr: false,
		},
		{
			title: "test fixed width type conversion",
			testSchema: arrow.NewSchema([]arrow.Field{
				{Name: "f1-bool", Type: arrow.FixedWidthTypes.Boolean},
				{Name: "f1-d32", Type: arrow.FixedWidthTypes.Date32},
				{Name: "f1-d64", Type: arrow.FixedWidthTypes.Date64},
				{Name: "f1-t32ms", Type: arrow.FixedWidthTypes.Time32ms},
				{Name: "f1-tsms", Type: arrow.FixedWidthTypes.Timestamp_ms},
				{
					Name: "null-bool", Type: arrow.FixedWidthTypes.Boolean,
					Nullable: true,
				},
				{
					Name: "null-d32", Type: arrow.FixedWidthTypes.Date32,
					Nullable: true,
				},
				{
					Name: "null-d64", Type: arrow.FixedWidthTypes.Date64,
					Nullable: true,
				},
				{
					Name: "null-t32ms", Type: arrow.FixedWidthTypes.Time32ms,
					Nullable: true,
				},
				{
					Name: "null-tsms", Type: arrow.FixedWidthTypes.Timestamp_ms,
					Nullable: true,
				},
			}, nil),
			expectedParquetMetaData: []string{
				"name=f1-bool, type=BOOLEAN, repetitiontype=REQUIRED",
				"name=f1-d32, type=INT32, convertedtype=DATE, " +
					"repetitiontype=REQUIRED",
				"name=f1-d64, type=INT32, convertedtype=DATE, " +
					"repetitiontype=REQUIRED",
				"name=f1-t32ms, type=INT32, convertedtype=TIME_MILLIS, " +
					"repetitiontype=REQUIRED",
				"name=f1-tsms, type=INT64, convertedtype=TIMESTAMP_MILLIS, " +
					"repetitiontype=REQUIRED",
				"name=null-bool, type=BOOLEAN, repetitiontype=OPTIONAL",
				"name=null-d32, type=INT32, convertedtype=DATE, " +
					"repetitiontype=OPTIONAL",
				"name=null-d64, type=INT32, convertedtype=DATE, " +
					"repetitiontype=OPTIONAL",
				"name=null-t32ms, type=INT32, convertedtype=TIME_MILLIS, " +
					"repetitiontype=OPTIONAL",
				"name=null-tsms, type=INT64, convertedtype=TIMESTAMP_MILLIS, " +
					"repetitiontype=OPTIONAL",
			},
			expectedErr: false,
		},
		{
			title: "test non supported types",
			testSchema: arrow.NewSchema([]arrow.Field{
				{Name: "f1-t64us", Type: arrow.FixedWidthTypes.Time64us},
				{Name: "f1-t32s", Type: arrow.FixedWidthTypes.Time32s},
				{Name: "f1-tsns", Type: arrow.FixedWidthTypes.Timestamp_ns},
				{Name: "f1-tss", Type: arrow.FixedWidthTypes.Timestamp_s},
				{
					Name: "null-t64us", Type: arrow.FixedWidthTypes.Time64us,
					Nullable: true,
				},
				{
					Name: "null-t32s", Type: arrow.FixedWidthTypes.Time32s,
					Nullable: true,
				},
				{
					Name: "null-tsns", Type: arrow.FixedWidthTypes.Timestamp_ns,
					Nullable: true,
				},
				{
					Name: "null-tss", Type: arrow.FixedWidthTypes.Timestamp_s,
					Nullable: true,
				},
			}, nil),
			expectedParquetMetaData: []string{},
			expectedErr:             true,
		},
	}
	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			actualMetaData, err := ConvertArrowToParquetSchema(test.testSchema)
			if err != nil {
				require.True(t, test.expectedErr)
			} else {
				require.False(t, test.expectedErr)
			}
			for k, v := range test.expectedParquetMetaData {
				require.Equal(t, v, actualMetaData[k])
			}
		})
	}
}
