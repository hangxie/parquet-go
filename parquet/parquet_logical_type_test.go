package parquet

import (
	"context"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
)

func Test_SimpleType(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{"StringType", func(t *testing.T) {
			st := NewStringType()
			require.NotNil(t, st)
			require.NotEmpty(t, st.String())
			st2 := NewStringType()
			require.True(t, st.Equals(st2))
		}},
		{"UUIDType", func(t *testing.T) {
			ut := NewUUIDType()
			require.NotNil(t, ut)
			require.NotEmpty(t, ut.String())
			ut2 := NewUUIDType()
			require.True(t, ut.Equals(ut2))
		}},
		{"MapType", func(t *testing.T) {
			mt := NewMapType()
			require.NotNil(t, mt)
			require.NotEmpty(t, mt.String())
			mt2 := NewMapType()
			require.True(t, mt.Equals(mt2))
		}},
		{"ListType", func(t *testing.T) {
			lt := NewListType()
			require.NotNil(t, lt)
			require.NotEmpty(t, lt.String())
			lt2 := NewListType()
			require.True(t, lt.Equals(lt2))
		}},
		{"EnumType", func(t *testing.T) {
			et := NewEnumType()
			require.NotNil(t, et)
			require.NotEmpty(t, et.String())
			et2 := NewEnumType()
			require.True(t, et.Equals(et2))
		}},
		{"DateType", func(t *testing.T) {
			dt := NewDateType()
			require.NotNil(t, dt)
			require.NotEmpty(t, dt.String())
			dt2 := NewDateType()
			require.True(t, dt.Equals(dt2))
		}},
		{"NullType", func(t *testing.T) {
			nt := NewNullType()
			require.NotNil(t, nt)
			require.NotEmpty(t, nt.String())
			nt2 := NewNullType()
			require.True(t, nt.Equals(nt2))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func Test_DecimalType(t *testing.T) {
	scale := int32(2)
	precision := int32(10)
	dt := NewDecimalType()
	dt.Scale = scale
	dt.Precision = precision
	require.Equal(t, scale, dt.GetScale())
	require.Equal(t, precision, dt.GetPrecision())

	str := dt.String()
	require.NotEmpty(t, str)

	dt2 := NewDecimalType()
	dt2.Scale = scale
	dt2.Precision = precision
	require.True(t, dt.Equals(dt2))
}

func Test_TimeUnit_Basic(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{"MilliSeconds", func(t *testing.T) {
			ms := NewMilliSeconds()
			require.NotNil(t, ms)
			require.NotEmpty(t, ms.String())
			ms2 := NewMilliSeconds()
			require.True(t, ms.Equals(ms2))
		}},
		{"MicroSeconds", func(t *testing.T) {
			us := NewMicroSeconds()
			require.NotNil(t, us)
			require.NotEmpty(t, us.String())
			us2 := NewMicroSeconds()
			require.True(t, us.Equals(us2))
		}},
		{"NanoSeconds", func(t *testing.T) {
			ns := NewNanoSeconds()
			require.NotNil(t, ns)
			require.NotEmpty(t, ns.String())
			ns2 := NewNanoSeconds()
			require.True(t, ns.Equals(ns2))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func Test_TimeUnit(t *testing.T) {
	tu := NewTimeUnit()

	millis := NewMilliSeconds()
	tu.MILLIS = millis
	require.Equal(t, millis, tu.GetMILLIS())
	require.True(t, tu.IsSetMILLIS())
	require.False(t, tu.IsSetMICROS())
	require.False(t, tu.IsSetNANOS())

	count := tu.CountSetFieldsTimeUnit()
	require.Equal(t, 1, count)

	str := tu.String()
	require.NotEmpty(t, str)

	tu2 := NewTimeUnit()
	tu2.MILLIS = millis
	require.True(t, tu.Equals(tu2))
}

func Test_TimestampType(t *testing.T) {
	isAdjusted := true
	tt := NewTimestampType()
	tt.IsAdjustedToUTC = isAdjusted

	unit := NewTimeUnit()
	unit.MILLIS = NewMilliSeconds()
	tt.Unit = unit
	require.Equal(t, isAdjusted, tt.GetIsAdjustedToUTC())
	require.Equal(t, unit, tt.GetUnit())
	require.True(t, tt.IsSetUnit())

	str := tt.String()
	require.NotEmpty(t, str)

	tt2 := NewTimestampType()
	tt2.IsAdjustedToUTC = isAdjusted
	tt2.Unit = unit
	require.True(t, tt.Equals(tt2))
}

func Test_TimeType(t *testing.T) {
	isAdjusted := false
	timeType := NewTimeType()
	timeType.IsAdjustedToUTC = isAdjusted

	unit := NewTimeUnit()
	unit.MICROS = NewMicroSeconds()
	timeType.Unit = unit
	require.Equal(t, isAdjusted, timeType.GetIsAdjustedToUTC())
	require.Equal(t, unit, timeType.GetUnit())
	require.True(t, timeType.IsSetUnit())

	str := timeType.String()
	require.NotEmpty(t, str)

	timeType2 := NewTimeType()
	timeType2.IsAdjustedToUTC = isAdjusted
	timeType2.Unit = unit
	require.True(t, timeType.Equals(timeType2))
}

func Test_IntType(t *testing.T) {
	bitWidth := int8(32)
	isSigned := true
	it := NewIntType()
	it.BitWidth = bitWidth
	it.IsSigned = isSigned
	require.Equal(t, bitWidth, it.GetBitWidth())
	require.Equal(t, isSigned, it.GetIsSigned())

	str := it.String()
	require.NotEmpty(t, str)

	it2 := NewIntType()
	it2.BitWidth = bitWidth
	it2.IsSigned = isSigned
	require.True(t, it.Equals(it2))
}

func Test_DocumentType(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{"JsonType", func(t *testing.T) {
			jt := NewJsonType()
			require.NotNil(t, jt)
			require.NotEmpty(t, jt.String())
			jt2 := NewJsonType()
			require.True(t, jt.Equals(jt2))
		}},
		{"BsonType", func(t *testing.T) {
			bt := NewBsonType()
			require.NotNil(t, bt)
			require.NotEmpty(t, bt.String())
			bt2 := NewBsonType()
			require.True(t, bt.Equals(bt2))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func Test_LogicalType(t *testing.T) {
	lt := NewLogicalType()
	require.NotNil(t, lt)

	stringType := NewStringType()
	lt.STRING = stringType
	require.Equal(t, stringType, lt.GetSTRING())
	require.True(t, lt.IsSetSTRING())
	require.False(t, lt.IsSetMAP())

	count := lt.CountSetFieldsLogicalType()
	require.Equal(t, 1, count)

	lt2 := NewLogicalType()
	mapType := NewMapType()
	lt2.MAP = mapType
	require.Equal(t, mapType, lt2.GetMAP())
	require.True(t, lt2.IsSetMAP())

	lt3 := NewLogicalType()
	listType := NewListType()
	lt3.LIST = listType
	require.Equal(t, listType, lt3.GetLIST())
	require.True(t, lt3.IsSetLIST())

	lt4 := NewLogicalType()
	enumType := NewEnumType()
	lt4.ENUM = enumType
	require.Equal(t, enumType, lt4.GetENUM())
	require.True(t, lt4.IsSetENUM())

	lt5 := NewLogicalType()
	decimalType := NewDecimalType()
	lt5.DECIMAL = decimalType
	require.Equal(t, decimalType, lt5.GetDECIMAL())
	require.True(t, lt5.IsSetDECIMAL())

	lt6 := NewLogicalType()
	dateType := NewDateType()
	lt6.DATE = dateType
	require.Equal(t, dateType, lt6.GetDATE())
	require.True(t, lt6.IsSetDATE())

	lt7 := NewLogicalType()
	timeType := NewTimeType()
	lt7.TIME = timeType
	require.Equal(t, timeType, lt7.GetTIME())
	require.True(t, lt7.IsSetTIME())

	lt8 := NewLogicalType()
	timestampType := NewTimestampType()
	lt8.TIMESTAMP = timestampType
	require.Equal(t, timestampType, lt8.GetTIMESTAMP())
	require.True(t, lt8.IsSetTIMESTAMP())

	lt9 := NewLogicalType()
	intType := NewIntType()
	lt9.INTEGER = intType
	require.Equal(t, intType, lt9.GetINTEGER())
	require.True(t, lt9.IsSetINTEGER())

	lt10 := NewLogicalType()
	lt10.UNKNOWN = &NullType{}
	require.NotNil(t, lt10.GetUNKNOWN())
	require.True(t, lt10.IsSetUNKNOWN())

	lt11 := NewLogicalType()
	jsonType := NewJsonType()
	lt11.JSON = jsonType
	require.Equal(t, jsonType, lt11.GetJSON())
	require.True(t, lt11.IsSetJSON())

	lt12 := NewLogicalType()
	bsonType := NewBsonType()
	lt12.BSON = bsonType
	require.Equal(t, bsonType, lt12.GetBSON())
	require.True(t, lt12.IsSetBSON())

	lt13 := NewLogicalType()
	uuidType := NewUUIDType()
	lt13.UUID = uuidType
	require.Equal(t, uuidType, lt13.GetUUID())
	require.True(t, lt13.IsSetUUID())

	str := lt.String()
	require.NotEmpty(t, str)

	ltCopy := NewLogicalType()
	ltCopy.STRING = stringType
	require.True(t, lt.Equals(ltCopy))
}

func Test_TimeUnit_Additional(t *testing.T) {
	tu := NewTimeUnit()

	micros := NewMicroSeconds()
	tu.MICROS = micros
	require.Equal(t, micros, tu.GetMICROS())

	tu2 := NewTimeUnit()
	nanos := NewNanoSeconds()
	tu2.NANOS = nanos
	require.Equal(t, nanos, tu2.GetNANOS())
}

func Test_GetterEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		getValue func() interface{}
	}{
		{"TimeUnit GetMICROS", func() interface{} { return NewTimeUnit().GetMICROS() }},
		{"TimeUnit GetNANOS", func() interface{} { return NewTimeUnit().GetNANOS() }},
		{"TimestampType GetUnit", func() interface{} { return NewTimestampType().GetUnit() }},
		{"TimeType GetUnit", func() interface{} { return NewTimeType().GetUnit() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.getValue()
			require.Nil(t, result)
		})
	}
}

func Test_CountSetFields(t *testing.T) {
	tu := NewTimeUnit()

	count := tu.CountSetFieldsTimeUnit()
	require.Equal(t, 0, count)

	tu.MILLIS = NewMilliSeconds()
	count = tu.CountSetFieldsTimeUnit()
	require.Equal(t, 1, count)

	bfa := NewBloomFilterAlgorithm()
	count = bfa.CountSetFieldsBloomFilterAlgorithm()
	require.Equal(t, 0, count)

	bfa.BLOCK = NewSplitBlockAlgorithm()
	count = bfa.CountSetFieldsBloomFilterAlgorithm()
	require.Equal(t, 1, count)

	lt := NewLogicalType()
	count = lt.CountSetFieldsLogicalType()
	require.Equal(t, 0, count)

	lt.STRING = NewStringType()
	count = lt.CountSetFieldsLogicalType()
	require.Equal(t, 1, count)
}

func Test_EqualsNilPointerCases(t *testing.T) {
	lt1 := NewLogicalType()
	lt1.DECIMAL = NewDecimalType()

	lt2 := NewLogicalType()
	require.False(t, lt1.Equals(lt2))
	tu1 := NewTimeUnit()
	tu1.MILLIS = NewMilliSeconds()
	tu2 := NewTimeUnit()
	tu2.MICROS = NewMicroSeconds()
	require.False(t, tu1.Equals(tu2))
	tt1 := NewTimestampType()
	tt1.IsAdjustedToUTC = true
	tt1.Unit = NewTimeUnit()
	tt1.Unit.MILLIS = NewMilliSeconds()
	tt2 := NewTimestampType()
	tt2.IsAdjustedToUTC = false // Different boolean
	tt2.Unit = NewTimeUnit()
	tt2.Unit.MILLIS = NewMilliSeconds()
	require.False(t, tt1.Equals(tt2))
}

func Test_StringMethodVariations(t *testing.T) {
	dt1 := NewDecimalType()
	str1 := dt1.String()

	dt1.Scale = 2
	dt1.Precision = 10
	str2 := dt1.String()
	require.NotEqual(t, str1, str2)

	it1 := NewIntType()
	str3 := it1.String()

	it1.BitWidth = 32
	it1.IsSigned = true
	str4 := it1.String()
	require.NotEqual(t, str3, str4)

	tu1 := NewTimeUnit()
	str5 := tu1.String()

	tu1.MILLIS = NewMilliSeconds()
	str6 := tu1.String()
	require.NotEqual(t, str5, str6)

	tu2 := NewTimeUnit()
	tu2.MICROS = NewMicroSeconds()
	str7 := tu2.String()
	require.NotEqual(t, str6, str7)
}

func Test_LogicalType_ReadField(t *testing.T) {
	ctx := context.Background()

	t.Run("ReadField2_MAP", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		originalLogicalType := NewLogicalType()
		originalLogicalType.MAP = NewMapType()
		require.NoError(t, originalLogicalType.Write(ctx, protocol))

		readTransport := thrift.NewTMemoryBuffer()
		readTransport.Buffer = transport.Buffer
		readProtocol := thrift.NewTBinaryProtocolConf(readTransport, nil)

		newLogicalType := NewLogicalType()
		require.NoError(t, newLogicalType.Read(ctx, readProtocol))
		require.NotNil(t, newLogicalType.MAP)
	})

	t.Run("ReadField3_LIST", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		originalLogicalType := NewLogicalType()
		originalLogicalType.LIST = NewListType()
		require.NoError(t, originalLogicalType.Write(ctx, protocol))

		readTransport := thrift.NewTMemoryBuffer()
		readTransport.Buffer = transport.Buffer
		readProtocol := thrift.NewTBinaryProtocolConf(readTransport, nil)

		newLogicalType := NewLogicalType()
		require.NoError(t, newLogicalType.Read(ctx, readProtocol))
		require.NotNil(t, newLogicalType.LIST)
	})

	t.Run("ReadField4_ENUM", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		originalLogicalType := NewLogicalType()
		originalLogicalType.ENUM = NewEnumType()
		require.NoError(t, originalLogicalType.Write(ctx, protocol))

		readTransport := thrift.NewTMemoryBuffer()
		readTransport.Buffer = transport.Buffer
		readProtocol := thrift.NewTBinaryProtocolConf(readTransport, nil)

		newLogicalType := NewLogicalType()
		require.NoError(t, newLogicalType.Read(ctx, readProtocol))
		require.NotNil(t, newLogicalType.ENUM)
	})

	t.Run("ReadField6_DATE", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		originalLogicalType := NewLogicalType()
		originalLogicalType.DATE = NewDateType()
		require.NoError(t, originalLogicalType.Write(ctx, protocol))

		readTransport := thrift.NewTMemoryBuffer()
		readTransport.Buffer = transport.Buffer
		readProtocol := thrift.NewTBinaryProtocolConf(readTransport, nil)

		newLogicalType := NewLogicalType()
		require.NoError(t, newLogicalType.Read(ctx, readProtocol))
		require.NotNil(t, newLogicalType.DATE)
	})
}

func Test_TimeUnit_GetMILLIS_EdgeCase(t *testing.T) {
	tu := NewTimeUnit()
	tu.MILLIS = nil

	result := tu.GetMILLIS()
	require.Nil(t, result)
}

func Test_LogicalType_Getters_NilCases(t *testing.T) {
	lt := NewLogicalType()

	require.Nil(t, lt.GetSTRING())
	require.Nil(t, lt.GetMAP())
	require.Nil(t, lt.GetLIST())
	require.Nil(t, lt.GetENUM())
	require.Nil(t, lt.GetDECIMAL())
	require.Nil(t, lt.GetDATE())
	require.Nil(t, lt.GetTIME())
	require.Nil(t, lt.GetTIMESTAMP())
	require.Nil(t, lt.GetINTEGER())
	require.Nil(t, lt.GetUNKNOWN())
	require.Nil(t, lt.GetJSON())
	require.Nil(t, lt.GetBSON())
	require.Nil(t, lt.GetUUID())
}

func Test_LogicalType_Write_Field(t *testing.T) {
	ctx := context.Background()

	t.Run("LogicalType_writeField2_MAP", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		lt := NewLogicalType()
		lt.MAP = NewMapType()

		err := lt.Write(ctx, protocol)
		require.NoError(t, err)

		readTransport := thrift.NewTMemoryBuffer()
		readTransport.Buffer = transport.Buffer
		readProtocol := thrift.NewTBinaryProtocolConf(readTransport, nil)

		newLt := NewLogicalType()
		err = newLt.Read(ctx, readProtocol)
		require.NoError(t, err)
		require.NotNil(t, newLt.MAP)
	})

	t.Run("LogicalType_writeField3_LIST", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		lt := NewLogicalType()
		lt.LIST = NewListType()

		err := lt.Write(ctx, protocol)
		require.NoError(t, err)

		readTransport := thrift.NewTMemoryBuffer()
		readTransport.Buffer = transport.Buffer
		readProtocol := thrift.NewTBinaryProtocolConf(readTransport, nil)

		newLt := NewLogicalType()
		err = newLt.Read(ctx, readProtocol)
		require.NoError(t, err)
		require.NotNil(t, newLt.LIST)
	})

	t.Run("LogicalType_writeField4_ENUM", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		lt := NewLogicalType()
		lt.ENUM = NewEnumType()

		err := lt.Write(ctx, protocol)
		require.NoError(t, err)

		readTransport := thrift.NewTMemoryBuffer()
		readTransport.Buffer = transport.Buffer
		readProtocol := thrift.NewTBinaryProtocolConf(readTransport, nil)

		newLt := NewLogicalType()
		err = newLt.Read(ctx, readProtocol)
		require.NoError(t, err)
		require.NotNil(t, newLt.ENUM)
	})

	t.Run("LogicalType_writeField6_DATE", func(t *testing.T) {
		transport := thrift.NewTMemoryBuffer()
		protocol := thrift.NewTBinaryProtocolConf(transport, nil)

		lt := NewLogicalType()
		lt.DATE = NewDateType()

		err := lt.Write(ctx, protocol)
		require.NoError(t, err)

		readTransport := thrift.NewTMemoryBuffer()
		readTransport.Buffer = transport.Buffer
		readProtocol := thrift.NewTBinaryProtocolConf(readTransport, nil)

		newLt := NewLogicalType()
		err = newLt.Read(ctx, readProtocol)
		require.NoError(t, err)
		require.NotNil(t, newLt.DATE)
	})
}

func Test_LogicalType_CountSetFields(t *testing.T) {
	lt := NewLogicalType()

	count := lt.CountSetFieldsLogicalType()
	require.Equal(t, 0, count)

	lt.STRING = NewStringType()
	count = lt.CountSetFieldsLogicalType()
	require.Equal(t, 1, count)

	lt.MAP = NewMapType()
	count = lt.CountSetFieldsLogicalType()
	require.Equal(t, 2, count)
}
