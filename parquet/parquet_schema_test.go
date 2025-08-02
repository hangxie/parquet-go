package parquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Statistics_Methods(t *testing.T) {
	stats := NewStatistics()
	require.NotNil(t, stats)

	maxVal := []byte("max")
	minVal := []byte("min")
	nullCount := int64(10)
	distinctCount := int64(100)
	maxValue := []byte("maxvalue")
	minValue := []byte("minvalue")

	stats.Max = maxVal
	stats.Min = minVal
	stats.NullCount = &nullCount
	stats.DistinctCount = &distinctCount
	stats.MaxValue = maxValue
	stats.MinValue = minValue
	require.Equal(t, maxVal, stats.GetMax())
	require.Equal(t, minVal, stats.GetMin())
	require.Equal(t, nullCount, stats.GetNullCount())
	require.Equal(t, distinctCount, stats.GetDistinctCount())
	require.Equal(t, maxValue, stats.GetMaxValue())
	require.Equal(t, minValue, stats.GetMinValue())
	require.True(t, stats.IsSetMax())
	require.True(t, stats.IsSetMin())
	require.True(t, stats.IsSetNullCount())
	require.True(t, stats.IsSetDistinctCount())
	require.True(t, stats.IsSetMaxValue())
	require.True(t, stats.IsSetMinValue())

	str := stats.String()
	require.NotEmpty(t, str)

	stats2 := NewStatistics()
	stats2.Max = maxVal
	stats2.Min = minVal
	stats2.NullCount = &nullCount
	stats2.DistinctCount = &distinctCount
	stats2.MaxValue = maxValue
	stats2.MinValue = minValue
	require.True(t, stats.Equals(stats2))

	differentNullCount := int64(20)
	stats2.NullCount = &differentNullCount
	require.False(t, stats.Equals(stats2))
}

func Test_StatisticsNilFields(t *testing.T) {
	stats := NewStatistics()

	require.Equal(t, int64(0), stats.GetNullCount())
	require.Equal(t, int64(0), stats.GetDistinctCount())
}

func Test_SchemaElement_Methods(t *testing.T) {
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

func Test_SchemaElementComprehensive(t *testing.T) {
	se := NewSchemaElement()

	require.Equal(t, Type(0), se.GetType())
	require.Equal(t, int32(0), se.GetTypeLength())
	require.Equal(t, FieldRepetitionType(0), se.GetRepetitionType())
	require.Equal(t, int32(0), se.GetNumChildren())
	require.Equal(t, ConvertedType(0), se.GetConvertedType())
	require.Equal(t, int32(0), se.GetScale())
	require.Equal(t, int32(0), se.GetPrecision())
	require.Equal(t, int32(0), se.GetFieldID())
	require.Nil(t, se.GetLogicalType())

	require.False(t, se.IsSetType())
	require.False(t, se.IsSetTypeLength())
	require.False(t, se.IsSetRepetitionType())
	require.False(t, se.IsSetNumChildren())
	require.False(t, se.IsSetConvertedType())
	require.False(t, se.IsSetScale())
	require.False(t, se.IsSetPrecision())
	require.False(t, se.IsSetFieldID())
	require.False(t, se.IsSetLogicalType())
}

func Test_SplitBlockAlgorithm_Methods(t *testing.T) {
	sba := NewSplitBlockAlgorithm()
	require.NotNil(t, sba)

	str := sba.String()
	require.NotEmpty(t, str)

	sba2 := NewSplitBlockAlgorithm()
	require.True(t, sba.Equals(sba2))
}

func Test_BloomFilterAlgorithm_Methods(t *testing.T) {
	bfa := NewBloomFilterAlgorithm()
	require.NotNil(t, bfa)

	block := NewSplitBlockAlgorithm()
	bfa.BLOCK = block
	require.Equal(t, block, bfa.GetBLOCK())
	require.True(t, bfa.IsSetBLOCK())

	count := bfa.CountSetFieldsBloomFilterAlgorithm()
	require.Equal(t, 1, count)

	str := bfa.String()
	require.NotEmpty(t, str)

	bfa2 := NewBloomFilterAlgorithm()
	bfa2.BLOCK = block
	require.True(t, bfa.Equals(bfa2))
}
