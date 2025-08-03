package parquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_SchemaElement(t *testing.T) {
	se := NewSchemaElement()
	require.NotNil(t, se)

	name := "test_field"
	se.Name = name
	require.Equal(t, name, se.GetName())

	fieldType := Type_INT32
	se.Type = &fieldType
	require.Equal(t, fieldType, se.GetType())
	require.True(t, se.IsSetType())

	typeLength := int32(10)
	se.TypeLength = &typeLength
	require.Equal(t, typeLength, se.GetTypeLength())
	require.True(t, se.IsSetTypeLength())

	repType := FieldRepetitionType_OPTIONAL
	se.RepetitionType = &repType
	require.Equal(t, repType, se.GetRepetitionType())
	require.True(t, se.IsSetRepetitionType())

	numChildren := int32(5)
	se.NumChildren = &numChildren
	require.Equal(t, numChildren, se.GetNumChildren())
	require.True(t, se.IsSetNumChildren())

	convertedType := ConvertedType_UTF8
	se.ConvertedType = &convertedType
	require.Equal(t, convertedType, se.GetConvertedType())
	require.True(t, se.IsSetConvertedType())

	scale := int32(2)
	se.Scale = &scale
	require.Equal(t, scale, se.GetScale())
	require.True(t, se.IsSetScale())

	precision := int32(10)
	se.Precision = &precision
	require.Equal(t, precision, se.GetPrecision())
	require.True(t, se.IsSetPrecision())

	fieldID := int32(1)
	se.FieldID = &fieldID
	require.Equal(t, fieldID, se.GetFieldID())
	require.True(t, se.IsSetFieldID())

	logicalType := NewLogicalType()
	logicalType.STRING = NewStringType()
	se.LogicalType = logicalType
	require.Equal(t, logicalType, se.GetLogicalType())
	require.True(t, se.IsSetLogicalType())

	str := se.String()
	require.NotEmpty(t, str)

	se2 := NewSchemaElement()
	se2.Name = name
	se2.Type = &fieldType
	se2.TypeLength = &typeLength
	se2.RepetitionType = &repType
	se2.NumChildren = &numChildren
	se2.ConvertedType = &convertedType
	se2.Scale = &scale
	se2.Precision = &precision
	se2.FieldID = &fieldID
	se2.LogicalType = logicalType
	require.True(t, se.Equals(se2))
}

func Test_SchemaElementDefaults(t *testing.T) {
	se := NewSchemaElement()
	require.Equal(t, int32(0), se.GetTypeLength())
	require.Equal(t, FieldRepetitionType(0), se.GetRepetitionType())
	require.Equal(t, int32(0), se.GetNumChildren())
	require.Equal(t, ConvertedType(0), se.GetConvertedType())
	require.Equal(t, int32(0), se.GetScale())
	require.Equal(t, int32(0), se.GetPrecision())
	require.Equal(t, int32(0), se.GetFieldID())
	require.Nil(t, se.GetLogicalType())
	require.False(t, se.IsSetTypeLength())
	require.False(t, se.IsSetRepetitionType())
	require.False(t, se.IsSetNumChildren())
	require.False(t, se.IsSetConvertedType())
	require.False(t, se.IsSetScale())
	require.False(t, se.IsSetPrecision())
	require.False(t, se.IsSetFieldID())
	require.False(t, se.IsSetLogicalType())
}

func Test_SchemaElement_Equals(t *testing.T) {
	tests := []struct {
		name     string
		element1 func() *SchemaElement
		element2 func() *SchemaElement
		expected bool
	}{
		{
			name: "same instance",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				se.Name = "test"
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.Name = "test"
				return se
			},
			expected: true,
		},
		{
			name: "nil first element",
			element1: func() *SchemaElement {
				return nil
			},
			element2: func() *SchemaElement {
				return NewSchemaElement()
			},
			expected: false,
		},
		{
			name: "nil second element",
			element1: func() *SchemaElement {
				return NewSchemaElement()
			},
			element2: func() *SchemaElement {
				return nil
			},
			expected: false,
		},
		{
			name: "both nil",
			element1: func() *SchemaElement {
				return nil
			},
			element2: func() *SchemaElement {
				return nil
			},
			expected: true,
		},
		{
			name: "different Type - one nil",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				typeVal := Type_INT32
				se.Type = &typeVal
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.Type = nil
				return se
			},
			expected: false,
		},
		{
			name: "different Type - different values",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				typeVal := Type_INT32
				se.Type = &typeVal
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				typeVal := Type_INT64
				se.Type = &typeVal
				return se
			},
			expected: false,
		},
		{
			name: "different TypeLength - one nil",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				length := int32(10)
				se.TypeLength = &length
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.TypeLength = nil
				return se
			},
			expected: false,
		},
		{
			name: "different TypeLength - different values",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				length := int32(10)
				se.TypeLength = &length
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				length := int32(20)
				se.TypeLength = &length
				return se
			},
			expected: false,
		},
		{
			name: "different RepetitionType - one nil",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				repType := FieldRepetitionType_REQUIRED
				se.RepetitionType = &repType
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.RepetitionType = nil
				return se
			},
			expected: false,
		},
		{
			name: "different RepetitionType - different values",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				repType := FieldRepetitionType_REQUIRED
				se.RepetitionType = &repType
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				repType := FieldRepetitionType_OPTIONAL
				se.RepetitionType = &repType
				return se
			},
			expected: false,
		},
		{
			name: "different Name",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				se.Name = "test1"
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.Name = "test2"
				return se
			},
			expected: false,
		},
		{
			name: "different NumChildren - one nil",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				numChildren := int32(5)
				se.NumChildren = &numChildren
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.NumChildren = nil
				return se
			},
			expected: false,
		},
		{
			name: "different ConvertedType - one nil",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				convertedType := ConvertedType_UTF8
				se.ConvertedType = &convertedType
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.ConvertedType = nil
				return se
			},
			expected: false,
		},
		{
			name: "different Scale - one nil",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				scale := int32(2)
				se.Scale = &scale
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.Scale = nil
				return se
			},
			expected: false,
		},
		{
			name: "different Precision - one nil",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				precision := int32(10)
				se.Precision = &precision
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.Precision = nil
				return se
			},
			expected: false,
		},
		{
			name: "different FieldID - one nil",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				fieldID := int32(1)
				se.FieldID = &fieldID
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.FieldID = nil
				return se
			},
			expected: false,
		},
		{
			name: "different LogicalType - one nil",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				se.LogicalType = NewLogicalType()
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.LogicalType = nil
				return se
			},
			expected: false,
		},
		{
			name: "identical elements",
			element1: func() *SchemaElement {
				se := NewSchemaElement()
				se.Name = "test"
				typeVal := Type_INT32
				se.Type = &typeVal
				length := int32(10)
				se.TypeLength = &length
				repType := FieldRepetitionType_REQUIRED
				se.RepetitionType = &repType
				numChildren := int32(0)
				se.NumChildren = &numChildren
				convertedType := ConvertedType_UTF8
				se.ConvertedType = &convertedType
				scale := int32(2)
				se.Scale = &scale
				precision := int32(10)
				se.Precision = &precision
				fieldID := int32(1)
				se.FieldID = &fieldID
				se.LogicalType = NewLogicalType()
				se.LogicalType.STRING = NewStringType()
				return se
			},
			element2: func() *SchemaElement {
				se := NewSchemaElement()
				se.Name = "test"
				typeVal := Type_INT32
				se.Type = &typeVal
				length := int32(10)
				se.TypeLength = &length
				repType := FieldRepetitionType_REQUIRED
				se.RepetitionType = &repType
				numChildren := int32(0)
				se.NumChildren = &numChildren
				convertedType := ConvertedType_UTF8
				se.ConvertedType = &convertedType
				scale := int32(2)
				se.Scale = &scale
				precision := int32(10)
				se.Precision = &precision
				fieldID := int32(1)
				se.FieldID = &fieldID
				se.LogicalType = NewLogicalType()
				se.LogicalType.STRING = NewStringType()
				return se
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			element1 := tt.element1()
			element2 := tt.element2()

			// Handle the case where we want to test the same instance
			if tt.name == "same instance" {
				element2 = element1
			}

			result := element1.Equals(element2)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_SchemaElement_GetType_EdgeCase(t *testing.T) {
	se := NewSchemaElement()
	se.Type = nil

	result := se.GetType()
	require.Equal(t, Type(0), result)
}
