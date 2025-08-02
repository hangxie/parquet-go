package parquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_StringType_Methods(t *testing.T) {
	st := NewStringType()
	require.NotNil(t, st)

	str := st.String()
	require.NotEmpty(t, str)

	st2 := NewStringType()
	require.True(t, st.Equals(st2))
}

func Test_UUIDType_Methods(t *testing.T) {
	ut := NewUUIDType()
	require.NotNil(t, ut)

	str := ut.String()
	require.NotEmpty(t, str)

	ut2 := NewUUIDType()
	require.True(t, ut.Equals(ut2))
}

func Test_MapType_Methods(t *testing.T) {
	mt := NewMapType()
	require.NotNil(t, mt)

	str := mt.String()
	require.NotEmpty(t, str)

	mt2 := NewMapType()
	require.True(t, mt.Equals(mt2))
}

func Test_ListType_Methods(t *testing.T) {
	lt := NewListType()
	require.NotNil(t, lt)

	str := lt.String()
	require.NotEmpty(t, str)

	lt2 := NewListType()
	require.True(t, lt.Equals(lt2))
}

func Test_EnumType_Methods(t *testing.T) {
	et := NewEnumType()
	require.NotNil(t, et)

	str := et.String()
	require.NotEmpty(t, str)

	et2 := NewEnumType()
	require.True(t, et.Equals(et2))
}

func Test_DateType_Methods(t *testing.T) {
	dt := NewDateType()
	require.NotNil(t, dt)

	str := dt.String()
	require.NotEmpty(t, str)

	dt2 := NewDateType()
	require.True(t, dt.Equals(dt2))
}

func Test_NullType_Methods(t *testing.T) {
	nt := NewNullType()
	require.NotNil(t, nt)

	str := nt.String()
	require.NotEmpty(t, str)

	nt2 := NewNullType()
	require.True(t, nt.Equals(nt2))
}

func Test_DecimalType_Methods(t *testing.T) {
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

func Test_MilliSeconds_Methods(t *testing.T) {
	ms := NewMilliSeconds()
	require.NotNil(t, ms)

	str := ms.String()
	require.NotEmpty(t, str)

	ms2 := NewMilliSeconds()
	require.True(t, ms.Equals(ms2))
}

func Test_MicroSeconds_Methods(t *testing.T) {
	us := NewMicroSeconds()
	require.NotNil(t, us)

	str := us.String()
	require.NotEmpty(t, str)

	us2 := NewMicroSeconds()
	require.True(t, us.Equals(us2))
}

func Test_NanoSeconds_Methods(t *testing.T) {
	ns := NewNanoSeconds()
	require.NotNil(t, ns)

	str := ns.String()
	require.NotEmpty(t, str)

	ns2 := NewNanoSeconds()
	require.True(t, ns.Equals(ns2))
}

func Test_TimeUnit_Methods(t *testing.T) {
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

func Test_TimestampType_Methods(t *testing.T) {
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

func Test_TimeType_Methods(t *testing.T) {
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

func Test_IntType_Methods(t *testing.T) {
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

func Test_JsonType_Methods(t *testing.T) {
	jt := NewJsonType()
	require.NotNil(t, jt)

	str := jt.String()
	require.NotEmpty(t, str)

	jt2 := NewJsonType()
	require.True(t, jt.Equals(jt2))
}

func Test_BsonType_Methods(t *testing.T) {
	bt := NewBsonType()
	require.NotNil(t, bt)

	str := bt.String()
	require.NotEmpty(t, str)

	bt2 := NewBsonType()
	require.True(t, bt.Equals(bt2))
}

func Test_LogicalType_Methods(t *testing.T) {
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

func Test_CountSetFieldsMethods(t *testing.T) {
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
	// Note: We can't test nil receiver cases because they would panic
	// Instead test the cases we can safely test

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
