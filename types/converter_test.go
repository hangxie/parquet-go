package types

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_DECIMAL(t *testing.T) {
	a1, _ := StrToParquetType("1.23", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 2)
	sa1 := DECIMAL_INT_ToString(int64(a1.(int32)), 9, 2)
	require.Equal(t, "1.23", sa1)

	a2, _ := StrToParquetType("1.230", parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa2 := DECIMAL_INT_ToString(int64(a2.(int64)), 9, 3)
	require.Equal(t, "1.230", sa2)

	a3, _ := StrToParquetType("11.230", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa3 := DECIMAL_BYTE_ARRAY_ToString([]byte(a3.(string)), 9, 3)
	require.Equal(t, "11.230", sa3)

	a4, _ := StrToParquetType("-123.456", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa4 := DECIMAL_BYTE_ARRAY_ToString([]byte(a4.(string)), 9, 3)
	require.Equal(t, "-123.456", sa4)

	a5, _ := StrToParquetType("0.000", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa5 := DECIMAL_BYTE_ARRAY_ToString([]byte(a5.(string)), 9, 3)
	require.Equal(t, "0.000", sa5)

	a6, _ := StrToParquetType("-0.01", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 6, 2)
	sa6 := DECIMAL_BYTE_ARRAY_ToString([]byte(a6.(string)), 6, 2)
	require.Equal(t, "-0.01", sa6)

	a7, _ := StrToParquetType("0.1234", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa7 := DECIMAL_BYTE_ARRAY_ToString([]byte(a7.(string)), 8, 4)
	require.Equal(t, "0.1234", sa7)

	a8, _ := StrToParquetType("-12.345", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 0, 3)
	sa8 := DECIMAL_INT_ToString(int64(a8.(int32)), 0, 3)
	require.Equal(t, "-12.345", sa8)

	a9, _ := StrToParquetType("-0.001", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 0, 3)
	sa9 := DECIMAL_INT_ToString(int64(a9.(int32)), 0, 3)
	require.Equal(t, "-0.001", sa9)

	a10, _ := StrToParquetType("0.0001", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 0, 4)
	sa10 := DECIMAL_INT_ToString(int64(a10.(int32)), 0, 4)
	require.Equal(t, "0.0001", sa10)

	a11, _ := StrToParquetType("-100000", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa11 := DECIMAL_BYTE_ARRAY_ToString([]byte(a11.(string)), 8, 4)
	require.Equal(t, "-100000.0000", sa11)

	a12, _ := StrToParquetType("100000", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa12 := DECIMAL_BYTE_ARRAY_ToString([]byte(a12.(string)), 8, 4)
	require.Equal(t, "100000.0000", sa12)

	a13, _ := StrToParquetType("-100", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa13 := DECIMAL_BYTE_ARRAY_ToString([]byte(a13.(string)), 8, 4)
	require.Equal(t, "-100.0000", sa13)

	a14, _ := StrToParquetType("100", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa14 := DECIMAL_BYTE_ARRAY_ToString([]byte(a14.(string)), 8, 4)
	require.Equal(t, "100.0000", sa14)

	a15, _ := StrToParquetType("-431", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa15 := DECIMAL_BYTE_ARRAY_ToString([]byte(a15.(string)), 8, 4)
	require.Equal(t, "-431.0000", sa15)

	a16, _ := StrToParquetType("431", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa16 := DECIMAL_BYTE_ARRAY_ToString([]byte(a16.(string)), 8, 4)
	require.Equal(t, "431.0000", sa16)

	a17, _ := StrToParquetType("-432", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa17 := DECIMAL_BYTE_ARRAY_ToString([]byte(a17.(string)), 8, 4)
	require.Equal(t, "-432.0000", sa17)

	a18, _ := StrToParquetType("432", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa18 := DECIMAL_BYTE_ARRAY_ToString([]byte(a18.(string)), 8, 4)
	require.Equal(t, "432.0000", sa18)

	a19, _ := StrToParquetType("-433", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa19 := DECIMAL_BYTE_ARRAY_ToString([]byte(a19.(string)), 8, 4)
	require.Equal(t, "-433.0000", sa19)

	a20, _ := StrToParquetType("433", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa20 := DECIMAL_BYTE_ARRAY_ToString([]byte(a20.(string)), 8, 4)
	require.Equal(t, "433.0000", sa20)

	a21, _ := StrToParquetType("-65535", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa21 := DECIMAL_BYTE_ARRAY_ToString([]byte(a21.(string)), 8, 4)
	require.Equal(t, "-65535.0000", sa21)

	a22, _ := StrToParquetType("65535", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa22 := DECIMAL_BYTE_ARRAY_ToString([]byte(a22.(string)), 8, 4)
	require.Equal(t, "65535.0000", sa22)
}

func Test_INT96(t *testing.T) {
	t1 := time.Now().Truncate(time.Microsecond).UTC()
	s := TimeToINT96(t1)
	t2 := INT96ToTime(s)

	require.True(t, t1.Equal(t2), "INT96 error: expected %v, got %v", t1, t2)
}

func Test_TIMESTAMP_MICROSToTime(t *testing.T) {
	micros := int64(1672574445123456) // Some microseconds since epoch

	result := TIMESTAMP_MICROSToTime(micros, true)
	expected := time.Unix(0, micros*int64(time.Microsecond)).UTC()
	require.True(t, result.Equal(expected), "TIMESTAMP_MICROSToTime(UTC=true) expected %v, got %v", expected, result)

	result2 := TIMESTAMP_MICROSToTime(micros, false)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
	expected2 := epoch.Add(time.Duration(micros) * time.Microsecond)
	require.True(t, result2.Equal(expected2), "TIMESTAMP_MICROSToTime(UTC=false) expected %v, got %v", expected2, result2)
}

func Test_TIMESTAMP_MILLISToTime(t *testing.T) {
	millis := int64(1672574445123) // Some milliseconds since epoch

	result := TIMESTAMP_MILLISToTime(millis, true)
	expected := time.Unix(0, millis*int64(time.Millisecond)).UTC()
	require.True(t, result.Equal(expected), "TIMESTAMP_MILLISToTime(UTC=true) expected %v, got %v", expected, result)

	result2 := TIMESTAMP_MILLISToTime(millis, false)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
	expected2 := epoch.Add(time.Duration(millis) * time.Millisecond)
	require.True(t, result2.Equal(expected2), "TIMESTAMP_MILLISToTime(UTC=false) expected %v, got %v", expected2, result2)
}

func Test_TIMESTAMP_NANOSToTime(t *testing.T) {
	nanos := int64(1672574445123456789) // Some nanoseconds since epoch

	result := TIMESTAMP_NANOSToTime(nanos, true)
	expected := time.Unix(0, nanos).UTC()
	require.True(t, result.Equal(expected), "TIMESTAMP_NANOSToTime(UTC=true) expected %v, got %v", expected, result)

	result2 := TIMESTAMP_NANOSToTime(nanos, false)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
	expected2 := epoch.Add(time.Duration(nanos))
	require.True(t, result2.Equal(expected2), "TIMESTAMP_NANOSToTime(UTC=false) expected %v, got %v", expected2, result2)
}

func Test_TimeToTIMESTAMP_MICROS(t *testing.T) {
	testTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.UTC)

	result := TimeToTIMESTAMP_MICROS(testTime, true)
	expected := testTime.UnixNano() / int64(time.Microsecond)
	require.Equal(t, expected, result, "TimeToTIMESTAMP_MICROS(UTC=true) expected %d, got %d", expected, result)

	localTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.Local)
	result2 := TimeToTIMESTAMP_MICROS(localTime, false)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, localTime.Location())
	expected2 := localTime.Sub(epoch).Nanoseconds() / int64(time.Microsecond)
	require.Equal(t, expected2, result2, "TimeToTIMESTAMP_MICROS(UTC=false) expected %d, got %d", expected2, result2)
}

func Test_TimeToTIMESTAMP_MILLIS(t *testing.T) {
	testTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.UTC)

	result := TimeToTIMESTAMP_MILLIS(testTime, true)
	expected := testTime.UnixNano() / int64(time.Millisecond)
	require.Equal(t, expected, result, "TimeToTIMESTAMP_MILLIS(UTC=true) expected %d, got %d", expected, result)

	localTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.Local)
	result2 := TimeToTIMESTAMP_MILLIS(localTime, false)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, localTime.Location())
	expected2 := localTime.Sub(epoch).Nanoseconds() / int64(time.Millisecond)
	require.Equal(t, expected2, result2, "TimeToTIMESTAMP_MILLIS(UTC=false) expected %d, got %d", expected2, result2)
}

func Test_TimeToTIMESTAMP_NANOS(t *testing.T) {
	testTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.UTC)

	result := TimeToTIMESTAMP_NANOS(testTime, true)
	expected := testTime.UnixNano()
	require.Equal(t, expected, result, "TimeToTIMESTAMP_NANOS(UTC=true) expected %d, got %d", expected, result)

	localTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.Local)
	result2 := TimeToTIMESTAMP_NANOS(localTime, false)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, localTime.Location())
	expected2 := localTime.Sub(epoch).Nanoseconds()
	require.Equal(t, expected2, result2, "TimeToTIMESTAMP_NANOS(UTC=false) expected %d, got %d", expected2, result2)
}

func Test_TimeToTIME_MICROS(t *testing.T) {
	testTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.UTC)

	result := TimeToTIME_MICROS(testTime, true)
	expected := int64(12*3600+30*60+45)*1000000 + 123456 // 45045123456 microseconds
	require.Equal(t, expected, result, "TimeToTIME_MICROS(UTC=true) expected %d, got %d", expected, result)

	localTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.Local)
	result2 := TimeToTIME_MICROS(localTime, false)
	expected2 := int64(12*3600+30*60+45)*1000000 + 123456
	require.Equal(t, expected2, result2, "TimeToTIME_MICROS(UTC=false) expected %d, got %d", expected2, result2)
}

func Test_TimeToTIME_MILLIS(t *testing.T) {
	testTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.UTC)

	result := TimeToTIME_MILLIS(testTime, true)
	expected := int64(12*3600+30*60+45)*1000 + 123 // 45045123 milliseconds
	require.Equal(t, expected, result, "TimeToTIME_MILLIS(UTC=true) expected %d, got %d", expected, result)

	localTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.Local)
	result2 := TimeToTIME_MILLIS(localTime, false)
	expected2 := int64(12*3600+30*60+45)*1000 + 123
	require.Equal(t, expected2, result2, "TimeToTIME_MILLIS(UTC=false) expected %d, got %d", expected2, result2)
}

func Test_IntervalToString(t *testing.T) {
	tests := []struct {
		name     string
		interval []byte
		expected string
	}{
		{
			name:     "zero_interval",
			interval: make([]byte, 12), // All zeros
			expected: "0.000 sec",
		},
		{
			name: "one_month_interval",
			interval: func() []byte {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 1)  // 1 month
				binary.LittleEndian.PutUint32(b[4:8], 0)  // 0 days
				binary.LittleEndian.PutUint32(b[8:12], 0) // 0 milliseconds
				return b
			}(),
			expected: "1 mon",
		},
		{
			name: "one_day_interval",
			interval: func() []byte {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 0)  // 0 months
				binary.LittleEndian.PutUint32(b[4:8], 1)  // 1 day
				binary.LittleEndian.PutUint32(b[8:12], 0) // 0 milliseconds
				return b
			}(),
			expected: "1 day",
		},
		{
			name: "one_hour_interval",
			interval: func() []byte {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 0)        // 0 months
				binary.LittleEndian.PutUint32(b[4:8], 0)        // 0 days
				binary.LittleEndian.PutUint32(b[8:12], 3600000) // 1 hour in milliseconds
				return b
			}(),
			expected: "3600.000 sec",
		},
		{
			name: "complex_interval",
			interval: func() []byte {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 2)        // 2 months
				binary.LittleEndian.PutUint32(b[4:8], 15)       // 15 days
				binary.LittleEndian.PutUint32(b[8:12], 7200000) // 2 hours in milliseconds
				return b
			}(),
			expected: "2 mon 15 day 7200.000 sec",
		},
		{
			name: "months_and_seconds_only",
			interval: func() []byte {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 3)     // 3 months
				binary.LittleEndian.PutUint32(b[4:8], 0)     // 0 days
				binary.LittleEndian.PutUint32(b[8:12], 1500) // 1.5 seconds in milliseconds
				return b
			}(),
			expected: "3 mon 1.500 sec",
		},
		{
			name: "days_and_seconds_only",
			interval: func() []byte {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 0)    // 0 months
				binary.LittleEndian.PutUint32(b[4:8], 7)    // 7 days
				binary.LittleEndian.PutUint32(b[8:12], 500) // 0.5 seconds in milliseconds
				return b
			}(),
			expected: "7 day 0.500 sec",
		},
		{
			name: "fractional_seconds",
			interval: func() []byte {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 0)   // 0 months
				binary.LittleEndian.PutUint32(b[4:8], 0)   // 0 days
				binary.LittleEndian.PutUint32(b[8:12], 25) // 0.025 seconds in milliseconds
				return b
			}(),
			expected: "0.025 sec",
		},
		{
			name:     "invalid_length_short",
			interval: []byte{1, 2, 3},
			expected: "",
		},
		{
			name:     "invalid_length_long",
			interval: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			expected: "",
		},
		{
			name:     "nil_interval",
			interval: nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IntervalToString(tt.interval)
			require.Equal(t, tt.expected, result)
		})
	}
}
