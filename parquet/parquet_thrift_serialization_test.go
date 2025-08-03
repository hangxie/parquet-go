package parquet

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
)

func Test_ThriftReadWrite(t *testing.T) {
	ctx := context.Background()

	stats := NewStatistics()
	stats.Max = []byte("max_value")
	stats.Min = []byte("min_value")
	nullCount := int64(100)
	stats.NullCount = &nullCount
	distinctCount := int64(50)
	stats.DistinctCount = &distinctCount
	stats.MaxValue = []byte("max_val")
	stats.MinValue = []byte("min_val")

	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTBinaryProtocolConf(transport, nil)

	require.NoError(t, stats.Write(ctx, protocol))

	newStats := NewStatistics()
	require.NoError(t, newStats.Read(ctx, protocol))
	require.Equal(t, stats.Min, newStats.Min)
	require.Equal(t, stats.NullCount, newStats.NullCount)
	require.Equal(t, stats.DistinctCount, newStats.DistinctCount)
	require.Equal(t, stats.MaxValue, newStats.MaxValue)
	require.Equal(t, stats.MinValue, newStats.MinValue)

	transport.Reset()
	decimal := NewDecimalType()
	decimal.Scale = 2
	decimal.Precision = 10
	require.NoError(t, decimal.Write(ctx, protocol))

	newDecimal := NewDecimalType()
	require.NoError(t, newDecimal.Read(ctx, protocol))
	require.Equal(t, decimal.Scale, newDecimal.Scale)
	require.Equal(t, decimal.Precision, newDecimal.Precision)

	transport.Reset()
	stringType := NewStringType()
	require.NoError(t, stringType.Write(ctx, protocol))

	newStringType := NewStringType()
	require.NoError(t, newStringType.Read(ctx, protocol))
	require.True(t, stringType.Equals(newStringType))

	transport.Reset()
	timeUnit := NewTimeUnit()
	timeUnit.MILLIS = NewMilliSeconds()
	require.NoError(t, timeUnit.Write(ctx, protocol))

	newTimeUnit := NewTimeUnit()
	require.NoError(t, newTimeUnit.Read(ctx, protocol))
	require.True(t, timeUnit.Equals(newTimeUnit))
	require.NotNil(t, newTimeUnit.MILLIS)

	transport.Reset()
	timestamp := NewTimestampType()
	timestamp.IsAdjustedToUTC = true
	timestamp.Unit = NewTimeUnit()
	timestamp.Unit.MICROS = NewMicroSeconds()
	require.NoError(t, timestamp.Write(ctx, protocol))

	newTimestamp := NewTimestampType()
	require.NoError(t, newTimestamp.Read(ctx, protocol))
	require.Equal(t, timestamp.IsAdjustedToUTC, newTimestamp.IsAdjustedToUTC)
	require.NotNil(t, newTimestamp.Unit)
	require.NotNil(t, newTimestamp.Unit.MICROS)

	transport.Reset()
	timeType := NewTimeType()
	timeType.IsAdjustedToUTC = false
	timeType.Unit = NewTimeUnit()
	timeType.Unit.NANOS = NewNanoSeconds()
	require.NoError(t, timeType.Write(ctx, protocol))

	newTimeType := NewTimeType()
	require.NoError(t, newTimeType.Read(ctx, protocol))
	require.Equal(t, timeType.IsAdjustedToUTC, newTimeType.IsAdjustedToUTC)
	require.NotNil(t, newTimeType.Unit)
	require.NotNil(t, newTimeType.Unit.NANOS)

	transport.Reset()
	intType := NewIntType()
	intType.BitWidth = 32
	intType.IsSigned = true
	require.NoError(t, intType.Write(ctx, protocol))

	newIntType := NewIntType()
	require.NoError(t, newIntType.Read(ctx, protocol))
	require.Equal(t, intType.BitWidth, newIntType.BitWidth)
	require.Equal(t, intType.IsSigned, newIntType.IsSigned)
}

func Test_ComplexThriftReadWrite(t *testing.T) {
	ctx := context.Background()
	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTBinaryProtocolConf(transport, nil)

	logicalType := NewLogicalType()
	logicalType.DECIMAL = NewDecimalType()
	logicalType.DECIMAL.Scale = 5
	logicalType.DECIMAL.Precision = 15
	require.NoError(t, logicalType.Write(ctx, protocol))

	newLogicalType := NewLogicalType()
	require.NoError(t, newLogicalType.Read(ctx, protocol))
	require.True(t, logicalType.Equals(newLogicalType))
	require.NotNil(t, newLogicalType.DECIMAL)
	require.Equal(t, int32(5), newLogicalType.DECIMAL.Scale)
	require.Equal(t, int32(15), newLogicalType.DECIMAL.Precision)

	transport.Reset()
	logicalType2 := NewLogicalType()
	logicalType2.TIME = NewTimeType()
	logicalType2.TIME.IsAdjustedToUTC = true
	logicalType2.TIME.Unit = NewTimeUnit()
	logicalType2.TIME.Unit.MILLIS = NewMilliSeconds()
	require.NoError(t, logicalType2.Write(ctx, protocol))

	newLogicalType2 := NewLogicalType()
	require.NoError(t, newLogicalType2.Read(ctx, protocol))
	require.True(t, logicalType2.Equals(newLogicalType2))
	require.NotNil(t, newLogicalType2.TIME)

	transport.Reset()
	schema := NewSchemaElement()
	schema.Name = "test_field"
	typeVal := Type_INT32
	schema.Type = &typeVal
	typeLength := int32(4)
	schema.TypeLength = &typeLength
	repType := FieldRepetitionType_REQUIRED
	schema.RepetitionType = &repType
	numChildren := int32(0)
	schema.NumChildren = &numChildren
	convertedType := ConvertedType_UTF8
	schema.ConvertedType = &convertedType
	scale := int32(2)
	schema.Scale = &scale
	precision := int32(10)
	schema.Precision = &precision
	fieldID := int32(1)
	schema.FieldID = &fieldID
	schema.LogicalType = NewLogicalType()
	schema.LogicalType.STRING = NewStringType()
	require.NoError(t, schema.Write(ctx, protocol))

	newSchema := NewSchemaElement()
	require.NoError(t, newSchema.Read(ctx, protocol))
	require.Equal(t, schema.Name, newSchema.Name)
	require.NotNil(t, newSchema.Type)
	require.Equal(t, *schema.Type, *newSchema.Type)
	require.NotNil(t, newSchema.TypeLength)
	require.Equal(t, *schema.TypeLength, *newSchema.TypeLength)
	require.NotNil(t, newSchema.RepetitionType)
	require.Equal(t, *schema.RepetitionType, *newSchema.RepetitionType)
	require.NotNil(t, newSchema.LogicalType)
	require.NotNil(t, newSchema.LogicalType.STRING)

	transport.Reset()
	pageHeader := NewDataPageHeader()
	pageHeader.NumValues = 1000
	pageHeader.Encoding = Encoding_PLAIN
	pageHeader.DefinitionLevelEncoding = Encoding_RLE
	pageHeader.RepetitionLevelEncoding = Encoding_RLE
	pageHeader.Statistics = NewStatistics()
	pageHeader.Statistics.Max = []byte("max")
	pageHeader.Statistics.Min = []byte("min")
	require.NoError(t, pageHeader.Write(ctx, protocol))

	newPageHeader := NewDataPageHeader()
	require.NoError(t, newPageHeader.Read(ctx, protocol))
	require.Equal(t, pageHeader.NumValues, newPageHeader.NumValues)
	require.Equal(t, pageHeader.Encoding, newPageHeader.Encoding)
	require.NotNil(t, newPageHeader.Statistics)
	require.True(t, bytes.Equal(newPageHeader.Statistics.Max, pageHeader.Statistics.Max))

	transport.Reset()
	dictHeader := NewDictionaryPageHeader()
	dictHeader.NumValues = 500
	dictHeader.Encoding = Encoding_PLAIN_DICTIONARY
	isSorted := true
	dictHeader.IsSorted = &isSorted
	require.NoError(t, dictHeader.Write(ctx, protocol))

	newDictHeader := NewDictionaryPageHeader()
	require.NoError(t, newDictHeader.Read(ctx, protocol))
	require.Equal(t, dictHeader.NumValues, newDictHeader.NumValues)
	require.Equal(t, dictHeader.Encoding, newDictHeader.Encoding)
	require.NotNil(t, newDictHeader.IsSorted)
	require.Equal(t, *dictHeader.IsSorted, *newDictHeader.IsSorted)

	transport.Reset()
	pageHeaderV2 := NewDataPageHeaderV2()
	pageHeaderV2.NumValues = 2000
	pageHeaderV2.NumNulls = 10
	pageHeaderV2.NumRows = 1990
	pageHeaderV2.Encoding = Encoding_PLAIN
	pageHeaderV2.DefinitionLevelsByteLength = 100
	pageHeaderV2.RepetitionLevelsByteLength = 50
	pageHeaderV2.IsCompressed = false
	pageHeaderV2.Statistics = NewStatistics()
	pageHeaderV2.Statistics.MaxValue = []byte("max_v2")
	require.NoError(t, pageHeaderV2.Write(ctx, protocol))

	newPageHeaderV2 := NewDataPageHeaderV2()
	require.NoError(t, newPageHeaderV2.Read(ctx, protocol))
	require.Equal(t, pageHeaderV2.NumValues, newPageHeaderV2.NumValues)
	require.Equal(t, pageHeaderV2.NumRows, newPageHeaderV2.NumRows)
	require.Equal(t, pageHeaderV2.IsCompressed, newPageHeaderV2.IsCompressed)
	require.NotNil(t, newPageHeaderV2.Statistics)
}

func Test_UnionAndComplexReadWrite(t *testing.T) {
	ctx := context.Background()
	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTBinaryProtocolConf(transport, nil)

	millis := NewMilliSeconds()
	require.NoError(t, millis.Write(ctx, protocol))

	newMillis := NewMilliSeconds()
	require.NoError(t, newMillis.Read(ctx, protocol))
	require.True(t, millis.Equals(newMillis))

	transport.Reset()
	micros := NewMicroSeconds()
	require.NoError(t, micros.Write(ctx, protocol))

	newMicros := NewMicroSeconds()
	require.NoError(t, newMicros.Read(ctx, protocol))
	require.True(t, micros.Equals(newMicros))

	transport.Reset()
	nanos := NewNanoSeconds()
	require.NoError(t, nanos.Write(ctx, protocol))

	newNanos := NewNanoSeconds()
	require.NoError(t, newNanos.Read(ctx, protocol))
	require.True(t, nanos.Equals(newNanos))

	transport.Reset()
	uuidType := NewUUIDType()
	require.NoError(t, uuidType.Write(ctx, protocol))

	newUUIDType := NewUUIDType()
	require.NoError(t, newUUIDType.Read(ctx, protocol))
	require.True(t, uuidType.Equals(newUUIDType))

	transport.Reset()
	mapType := NewMapType()
	require.NoError(t, mapType.Write(ctx, protocol))

	newMapType := NewMapType()
	require.NoError(t, newMapType.Read(ctx, protocol))
	require.True(t, mapType.Equals(newMapType))

	transport.Reset()
	listType := NewListType()
	require.NoError(t, listType.Write(ctx, protocol))

	newListType := NewListType()
	require.NoError(t, newListType.Read(ctx, protocol))
	require.True(t, listType.Equals(newListType))

	transport.Reset()
	enumType := NewEnumType()
	require.NoError(t, enumType.Write(ctx, protocol))

	newEnumType := NewEnumType()
	require.NoError(t, newEnumType.Read(ctx, protocol))
	require.True(t, enumType.Equals(newEnumType))

	transport.Reset()
	dateType := NewDateType()
	require.NoError(t, dateType.Write(ctx, protocol))

	newDateType := NewDateType()
	require.NoError(t, newDateType.Read(ctx, protocol))
	require.True(t, dateType.Equals(newDateType))

	transport.Reset()
	nullType := NewNullType()
	require.NoError(t, nullType.Write(ctx, protocol))

	newNullType := NewNullType()
	require.NoError(t, newNullType.Read(ctx, protocol))
	require.True(t, nullType.Equals(newNullType))

	transport.Reset()
	jsonType := NewJsonType()
	require.NoError(t, jsonType.Write(ctx, protocol))

	newJsonType := NewJsonType()
	require.NoError(t, newJsonType.Read(ctx, protocol))
	require.True(t, jsonType.Equals(newJsonType))

	transport.Reset()
	bsonType := NewBsonType()
	require.NoError(t, bsonType.Write(ctx, protocol))

	newBsonType := NewBsonType()
	require.NoError(t, newBsonType.Read(ctx, protocol))
	require.True(t, bsonType.Equals(newBsonType))

	transport.Reset()
	indexHeader := NewIndexPageHeader()
	require.NoError(t, indexHeader.Write(ctx, protocol))

	newIndexHeader := NewIndexPageHeader()
	require.NoError(t, newIndexHeader.Read(ctx, protocol))
	require.True(t, indexHeader.Equals(newIndexHeader))

	transport.Reset()
	splitBlock := NewSplitBlockAlgorithm()
	require.NoError(t, splitBlock.Write(ctx, protocol))

	newSplitBlock := NewSplitBlockAlgorithm()
	require.NoError(t, newSplitBlock.Read(ctx, protocol))
	require.True(t, splitBlock.Equals(newSplitBlock))

	transport.Reset()
	bloomFilter := NewBloomFilterAlgorithm()
	bloomFilter.BLOCK = NewSplitBlockAlgorithm()
	require.NoError(t, bloomFilter.Write(ctx, protocol))

	newBloomFilter := NewBloomFilterAlgorithm()
	require.NoError(t, newBloomFilter.Read(ctx, protocol))
	require.True(t, bloomFilter.Equals(newBloomFilter))
	require.NotNil(t, newBloomFilter.BLOCK)
}

func Test_Thrift_Read_Error_Handling(t *testing.T) {
	ctx := context.Background()

	t.Run("StringType_Read_Error", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		transport.WriteString("invalid thrift data")
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		st := NewStringType()
		err := st.Read(ctx, protocol)
		require.Error(t, err)
	})

	t.Run("Statistics_Read_Error", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		transport.WriteString("invalid thrift data")
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		stats := NewStatistics()
		err := stats.Read(ctx, protocol)
		require.Error(t, err)
	})

	t.Run("LogicalType_Read_Error", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		transport.WriteString("invalid thrift data")
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		lt := NewLogicalType()
		err := lt.Read(ctx, protocol)
		require.Error(t, err)
	})
}
