package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestConvertTimestampValue_Nil(t *testing.T) {
	result := ConvertTimestampValue(nil, parquet.ConvertedType_TIMESTAMP_MILLIS)
	require.Nil(t, result)
}

func TestConvertTimestampValue_MillisNonInt64(t *testing.T) {
	// non-int64 value should be returned as-is
	result := ConvertTimestampValue("not-an-int", parquet.ConvertedType_TIMESTAMP_MILLIS)
	require.Equal(t, "not-an-int", result)
}

func TestConvertTimestampValue_Millis(t *testing.T) {
	// Epoch in millis = 0 → 1970-01-01T00:00:00Z
	result := ConvertTimestampValue(int64(0), parquet.ConvertedType_TIMESTAMP_MILLIS)
	require.IsType(t, "", result)
	require.Contains(t, result.(string), "1970-01-01")
}

func TestConvertTimestampValue_Micros(t *testing.T) {
	result := ConvertTimestampValue(int64(0), parquet.ConvertedType_TIMESTAMP_MICROS)
	require.IsType(t, "", result)
	require.Contains(t, result.(string), "1970-01-01")
}

func TestConvertTimestampValue_UnknownConvertedType(t *testing.T) {
	// For an unsupported ConvertedType, return value unchanged
	result := ConvertTimestampValue(int64(12345), parquet.ConvertedType_INT_8)
	require.Equal(t, int64(12345), result)
}

func TestTIMESTAMP_MICROSToTime(t *testing.T) {
	micros := int64(1672574445123456)

	result := TIMESTAMP_MICROSToTime(micros, true)
	expected := time.Unix(0, micros*int64(time.Microsecond)).UTC()
	require.True(t, result.Equal(expected), "TIMESTAMP_MICROSToTime(UTC=true) expected %v, got %v", expected, result)

	result2 := TIMESTAMP_MICROSToTime(micros, false)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
	expected2 := epoch.Add(time.Duration(micros) * time.Microsecond)
	require.True(t, result2.Equal(expected2), "TIMESTAMP_MICROSToTime(UTC=false) expected %v, got %v", expected2, result2)
}

func TestTIMESTAMP_MILLISToTime(t *testing.T) {
	millis := int64(1672574445123)

	result := TIMESTAMP_MILLISToTime(millis, true)
	expected := time.Unix(0, millis*int64(time.Millisecond)).UTC()
	require.True(t, result.Equal(expected), "TIMESTAMP_MILLISToTime(UTC=true) expected %v, got %v", expected, result)

	result2 := TIMESTAMP_MILLISToTime(millis, false)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
	expected2 := epoch.Add(time.Duration(millis) * time.Millisecond)
	require.True(t, result2.Equal(expected2), "TIMESTAMP_MILLISToTime(UTC=false) expected %v, got %v", expected2, result2)
}

func TestTIMESTAMP_NANOSToTime(t *testing.T) {
	nanos := int64(1672574445123456789)

	result := TIMESTAMP_NANOSToTime(nanos, true)
	expected := time.Unix(0, nanos).UTC()
	require.True(t, result.Equal(expected), "TIMESTAMP_NANOSToTime(UTC=true) expected %v, got %v", expected, result)

	result2 := TIMESTAMP_NANOSToTime(nanos, false)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
	expected2 := epoch.Add(time.Duration(nanos))
	require.True(t, result2.Equal(expected2), "TIMESTAMP_NANOSToTime(UTC=false) expected %v, got %v", expected2, result2)
}

func TestTimeToTIMESTAMP_MICROS(t *testing.T) {
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

func TestTimeToTIMESTAMP_MILLIS(t *testing.T) {
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

func TestTimeToTIMESTAMP_NANOS(t *testing.T) {
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
