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

// TestTimestampSpellingsAgree covers the two ways a TIMESTAMP can be annotated. The logical
// one kept fmt.Sscanf, so it stored "123abc" as 123 where the converted one rejected it.
func TestTimestampSpellingsAgree(t *testing.T) {
	int64T := parquet.Type_INT64

	units := []struct {
		name  string
		cT    parquet.ConvertedType
		logic *parquet.LogicalType
	}{
		{"MILLIS", parquet.ConvertedType_TIMESTAMP_MILLIS, createTimestampLogicalType(true, false, false, true)},
		{"MICROS", parquet.ConvertedType_TIMESTAMP_MICROS, createTimestampLogicalType(false, true, false, true)},
	}

	for _, unit := range units {
		t.Run(unit.name, func(t *testing.T) {
			cT := parquet.ConvertedTypePtr(unit.cT)

			for _, text := range []string{"123abc", "1.5", "NaN", ""} {
				_, logicalErr := StrToParquetTypeWithLogical(text, &int64T, nil, unit.logic, 0, 0)
				_, convertedErr := StrToParquetTypeWithLogical(text, &int64T, cT, nil, 0, 0)
				require.Error(t, logicalErr, "logical %q", text)
				require.Error(t, convertedErr, "converted %q", text)
			}

			// A value both spellings hold reads the same, in either form.
			for _, text := range []string{"1699999999999", "-1", " 42 ", "2023-12-25T00:00:00Z"} {
				fromLogical, err := StrToParquetTypeWithLogical(text, &int64T, nil, unit.logic, 0, 0)
				require.NoError(t, err, "logical %q", text)
				fromConverted, err := StrToParquetTypeWithLogical(text, &int64T, cT, nil, 0, 0)
				require.NoError(t, err, "converted %q", text)
				require.Equal(t, fromConverted, fromLogical, "%q", text)
			}
		})
	}

	// NANOS has no converted spelling, but its tick count is scanned the same way.
	nanos := createTimestampLogicalType(false, false, true, true)
	_, err := StrToParquetTypeWithLogical("123abc", &int64T, nil, nanos, 0, 0)
	require.ErrorContains(t, err, `parse TIMESTAMP_NANOS "123abc"`)
}

// TestTimestampUnsetUnit covers a TIMESTAMP whose unit union carries no field, as a file
// naming an unknown unit decodes. It says as little as a missing unit, so both behave alike.
func TestTimestampUnsetUnit(t *testing.T) {
	int64T := parquet.Type_INT64

	unsetUnion := parquet.NewLogicalType()
	unsetUnion.TIMESTAMP = &parquet.TimestampType{IsAdjustedToUTC: true, Unit: parquet.NewTimeUnit()}
	nilUnit := parquet.NewLogicalType()
	nilUnit.TIMESTAMP = &parquet.TimestampType{IsAdjustedToUTC: true}

	for name, lT := range map[string]*parquet.LogicalType{"unset union": unsetUnion, "nil unit": nilUnit} {
		t.Run(name, func(t *testing.T) {
			// The bare tick count the column stores is read as the column's own INT64.
			got, err := StrToParquetTypeWithLogical("12345", &int64T, nil, lT, 0, 0)
			require.NoError(t, err)
			require.Equal(t, int64(12345), got)

			// Timestamp text cannot be scaled without a unit, and both spellings say so
			// the same way.
			_, err = StrToParquetTypeWithLogical("2023-12-25T00:00:00Z", &int64T, nil, lT, 0, 0)
			require.ErrorContains(t, err, `parse INT64 "2023-12-25T00:00:00Z"`)
		})
	}
}

// TestTimestampRangeBeyondNanoseconds covers timestamps outside the nanosecond window,
// roughly 1678 to 2262. Scaling every unit from UnixNano wrapped there, storing
// "2300-01-01T00:00:00Z" in a MILLIS column as a count reading back as 1715.
func TestTimestampRangeBeyondNanoseconds(t *testing.T) {
	int64T := parquet.Type_INT64

	units := []struct {
		name string
		cT   parquet.ConvertedType
		lT   *parquet.LogicalType
		want func(time.Time) int64
	}{
		{"MILLIS", parquet.ConvertedType_TIMESTAMP_MILLIS, createTimestampLogicalType(true, false, false, true), time.Time.UnixMilli},
		{"MICROS", parquet.ConvertedType_TIMESTAMP_MICROS, createTimestampLogicalType(false, true, false, true), time.Time.UnixMicro},
	}

	for _, unit := range units {
		t.Run(unit.name, func(t *testing.T) {
			cT := parquet.ConvertedTypePtr(unit.cT)
			for _, text := range []string{
				"2300-01-01T00:00:00Z",      // past the nanosecond ceiling
				"1600-01-01T00:00:00Z",      // before its floor
				"1969-12-31T23:59:59.9995Z", // sub-unit precision below the epoch
				"2023-12-25T00:00:00Z",      // well inside it
			} {
				parsed, err := time.Parse(time.RFC3339Nano, text)
				require.NoError(t, err)

				fromConverted, err := StrToParquetTypeWithLogical(text, &int64T, cT, nil, 0, 0)
				require.NoError(t, err, text)
				require.Equal(t, unit.want(parsed), fromConverted, text)

				fromLogical, err := StrToParquetTypeWithLogical(text, &int64T, nil, unit.lT, 0, 0)
				require.NoError(t, err, text)
				require.Equal(t, fromConverted, fromLogical, text)
			}
		})
	}

	// NANOS genuinely cannot hold those instants, so it reports them instead of wrapping.
	nanos := createTimestampLogicalType(false, false, true, true)
	for _, text := range []string{"2300-01-01T00:00:00Z", "1600-01-01T00:00:00Z"} {
		_, err := StrToParquetTypeWithLogical(text, &int64T, nil, nanos, 0, 0)
		require.ErrorContains(t, err, "outside the range a nanosecond count can hold", text)
	}

	got, err := StrToParquetTypeWithLogical("2023-12-25T00:00:00Z", &int64T, nil, nanos, 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1703462400000000000), got)
}
