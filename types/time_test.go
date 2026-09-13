package types

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestTimeToTIME_MICROS(t *testing.T) {
	testTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.UTC)

	result := TimeToTIME_MICROS(testTime, true)
	expected := int64(12*3600+30*60+45)*1000000 + 123456 // 45045123456 microseconds
	require.Equal(t, expected, result, "TimeToTIME_MICROS(UTC=true) expected %d, got %d", expected, result)

	localTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.Local)
	result2 := TimeToTIME_MICROS(localTime, false)
	expected2 := int64(12*3600+30*60+45)*1000000 + 123456
	require.Equal(t, expected2, result2, "TimeToTIME_MICROS(UTC=false) expected %d, got %d", expected2, result2)
}

func TestTimeToTIME_MILLIS(t *testing.T) {
	testTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.UTC)

	result := TimeToTIME_MILLIS(testTime, true)
	expected := int64(12*3600+30*60+45)*1000 + 123 // 45045123 milliseconds
	require.Equal(t, expected, result, "TimeToTIME_MILLIS(UTC=true) expected %d, got %d", expected, result)

	localTime := time.Date(2023, 1, 1, 12, 30, 45, 123456789, time.Local)
	result2 := TimeToTIME_MILLIS(localTime, false)
	expected2 := int64(12*3600+30*60+45)*1000 + 123
	require.Equal(t, expected2, result2, "TimeToTIME_MILLIS(UTC=false) expected %d, got %d", expected2, result2)
}

func TestTIME_MILLISToTimeFormat(t *testing.T) {
	tests := []struct {
		name     string
		millis   int32
		expected string
	}{
		{
			name:     "zero time",
			millis:   0,
			expected: "00:00:00.000",
		},
		{
			name:     "12:34:56.789",
			millis:   45296789, // 12*3600*1000 + 34*60*1000 + 56*1000 + 789
			expected: "12:34:56.789",
		},
		{
			name:     "00:00:01.001",
			millis:   1001,
			expected: "00:00:01.001",
		},
		{
			name:     "23:59:59.999",
			millis:   86399999, // 23*3600*1000 + 59*60*1000 + 59*1000 + 999
			expected: "23:59:59.999",
		},
		{
			name:     "09:05:03.123",
			millis:   32703123, // 9*3600*1000 + 5*60*1000 + 3*1000 + 123
			expected: "09:05:03.123",
		},
		{
			// Out-of-range values are not writable but can arrive from another writer; the
			// sign belongs to the whole value, not to one component as in "00:00:-1.000".
			name:     "negative one second",
			millis:   -1000,
			expected: "-00:00:01.000",
		},
		{
			name:     "past midnight",
			millis:   90000000, // 25 hours
			expected: "25:00:00.000",
		},
		{
			name:     "minimum int32",
			millis:   math.MinInt32,
			expected: "-596:31:23.648",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TIME_MILLISToTimeFormat(tt.millis)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestTIME_MICROSToTimeFormat(t *testing.T) {
	tests := []struct {
		name     string
		micros   int64
		expected string
	}{
		{
			name:     "zero time",
			micros:   0,
			expected: "00:00:00.000000",
		},
		{
			name:     "12:34:56.789012",
			micros:   45296789012, // 12*3600*1000000 + 34*60*1000000 + 56*1000000 + 789012
			expected: "12:34:56.789012",
		},
		{
			name:     "00:00:01.000001",
			micros:   1000001,
			expected: "00:00:01.000001",
		},
		{
			name:     "23:59:59.999999",
			micros:   86399999999, // 23*3600*1000000 + 59*60*1000000 + 59*1000000 + 999999
			expected: "23:59:59.999999",
		},
		{
			name:     "09:05:03.123456",
			micros:   32703123456, // 9*3600*1000000 + 5*60*1000000 + 3*1000000 + 123456
			expected: "09:05:03.123456",
		},
		{
			name:     "negative one second",
			micros:   -1000000,
			expected: "-00:00:01.000000",
		},
		{
			name:     "past midnight",
			micros:   90000000000, // 25 hours
			expected: "25:00:00.000000",
		},
		{
			// Converting to nanoseconds first overflowed here and printed a value with no
			// relation to the input.
			name:     "minimum int64",
			micros:   math.MinInt64,
			expected: "-2562047788:00:54.775808",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TIME_MICROSToTimeFormat(tt.micros)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertTimeLogicalValue_Nil(t *testing.T) {
	result := ConvertTimeLogicalValue(nil, nil)
	require.Nil(t, result)
}

func TestConvertTimeLogicalValue_NilTimeType(t *testing.T) {
	result := ConvertTimeLogicalValue(int32(1000), nil)
	require.Equal(t, int32(1000), result)
}

func TestConvertTimeLogicalValue_MILLIS(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.MILLIS = parquet.NewMilliSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	result := ConvertTimeLogicalValue(int32(3661000), timeType)
	require.IsType(t, "", result)
	require.Contains(t, result.(string), "01:01:01")
}

func TestConvertTimeLogicalValue_MILLIS_WrongValueType(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.MILLIS = parquet.NewMilliSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	result := ConvertTimeLogicalValue(int64(1000), timeType)
	require.Equal(t, int64(1000), result)
}

func TestConvertTimeLogicalValue_MICROS(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.MICROS = parquet.NewMicroSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	result := ConvertTimeLogicalValue(int64(3661000000), timeType)
	require.IsType(t, "", result)
	require.Contains(t, result.(string), "01:01:01")
}

func TestConvertTimeLogicalValue_MICROS_WrongValueType(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.MICROS = parquet.NewMicroSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	result := ConvertTimeLogicalValue(int32(1000), timeType)
	require.Equal(t, int32(1000), result)
}

func TestConvertTimeLogicalValue_NANOS(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.NANOS = parquet.NewNanoSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	result := ConvertTimeLogicalValue(int64(3661000000000), timeType)
	require.IsType(t, "", result)
	require.Contains(t, result.(string), "01:01:01")
}

func TestConvertTimeLogicalValue_NANOS_WrongValueType(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.NANOS = parquet.NewNanoSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	result := ConvertTimeLogicalValue(int32(100), timeType)
	require.Equal(t, int32(100), result)
}

func TestConvertTimeLogicalValue_NilUnit(t *testing.T) {
	timeType := &parquet.TimeType{Unit: nil}
	result := ConvertTimeLogicalValue(int32(1000), timeType)
	require.Equal(t, int32(1000), result)
}

func TestParseTimeString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
		errMsg   string
	}{
		{
			name:     "time_millis",
			input:    "12:34:56.789",
			expected: 45296789000000, // nanoseconds
		},
		{
			name:     "time_micros",
			input:    "12:34:56.789012",
			expected: 45296789012000, // nanoseconds
		},
		{
			name:     "time_nanos",
			input:    "12:34:56.789012345",
			expected: 45296789012345, // nanoseconds
		},
		{
			name:     "time_no_fraction",
			input:    "12:34:56",
			expected: 45296000000000, // nanoseconds
		},
		{
			name:     "midnight",
			input:    "00:00:00.000",
			expected: 0,
		},
		{
			name:   "invalid_format",
			input:  "12-34-56",
			errMsg: "cannot parse time string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseTimeString(tt.input)
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestTIMESTAMP_MILLISToISO8601(t *testing.T) {
	tests := []struct {
		name          string
		millis        int64
		adjustedToUTC bool
		expected      string
	}{
		{
			name:          "epoch_time",
			millis:        0,
			adjustedToUTC: true,
			expected:      "1970-01-01T00:00:00.000Z",
		},
		{
			name:          "new_year_2022",
			millis:        1640995200000, // 2022-01-01T00:00:00Z
			adjustedToUTC: true,
			expected:      "2022-01-01T00:00:00.000Z",
		},
		{
			name:          "with_milliseconds",
			millis:        1640995200123, // 2022-01-01T00:00:00.123Z
			adjustedToUTC: true,
			expected:      "2022-01-01T00:00:00.123Z",
		},
		{
			name:          "past_timestamp",
			millis:        946684800000, // 2000-01-01T00:00:00Z
			adjustedToUTC: true,
			expected:      "2000-01-01T00:00:00.000Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TIMESTAMP_MILLISToISO8601(tt.millis, tt.adjustedToUTC)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestTIMESTAMP_MICROSToISO8601(t *testing.T) {
	tests := []struct {
		name          string
		micros        int64
		adjustedToUTC bool
		expected      string
	}{
		{
			name:          "epoch_time",
			micros:        0,
			adjustedToUTC: true,
			expected:      "1970-01-01T00:00:00.000000Z",
		},
		{
			name:          "new_year_2022",
			micros:        1640995200000000, // 2022-01-01T00:00:00Z
			adjustedToUTC: true,
			expected:      "2022-01-01T00:00:00.000000Z",
		},
		{
			name:          "with_microseconds",
			micros:        1640995200123456, // 2022-01-01T00:00:00.123456Z
			adjustedToUTC: true,
			expected:      "2022-01-01T00:00:00.123456Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TIMESTAMP_MICROSToISO8601(tt.micros, tt.adjustedToUTC)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertTimeLogicalValue_NANOS_OutOfRange(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.NANOS = parquet.NewNanoSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	require.Equal(t, "-00:00:00.000000001", ConvertTimeLogicalValue(int64(-1), timeType))
	require.Equal(t, "24:00:00.000000000", ConvertTimeLogicalValue(int64(86400000000000), timeType))
}

func TestParseTimeOfDay(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		unit     time.Duration
		expected int64
		errMsg   string
	}{
		{name: "clock_string", s: "12:34:56.789", unit: time.Millisecond, expected: 45296789},
		{name: "clock_string_micros", s: "12:34:56.789012", unit: time.Microsecond, expected: 45296789012},
		{name: "ticks", s: "45296789", unit: time.Millisecond, expected: 45296789},
		{name: "ticks_padded", s: " 45296789 ", unit: time.Millisecond, expected: 45296789},
		{name: "last_tick_of_day", s: "86399999", unit: time.Millisecond, expected: 86399999},
		{name: "midnight", s: "0", unit: time.Nanosecond, expected: 0},
		{name: "negative", s: "-1000", unit: time.Millisecond, errMsg: "outside [0, 24h)"},
		{name: "full_day", s: "86400000", unit: time.Millisecond, errMsg: "outside [0, 24h)"},
		{name: "past_midnight", s: "90000000", unit: time.Millisecond, errMsg: "outside [0, 24h)"},
		// The reader's own rendering of an out-of-range value: Sscanf used to keep the 25
		// and drop the rest, turning 25 hours into 25 milliseconds.
		{name: "out_of_range_clock_string", s: "25:00:00.000", unit: time.Millisecond, errMsg: "parse TIME_MILLIS"},
		{name: "negative_clock_string", s: "-00:00:01.000", unit: time.Millisecond, errMsg: "parse TIME_MILLIS"},
		{name: "trailing_garbage", s: "12abc", unit: time.Millisecond, errMsg: "parse TIME_MILLIS"},
		// Too wide for the column: the range is what went wrong, not the scan.
		{name: "wider_than_int64", s: "9223372036854775808", unit: time.Millisecond, errMsg: "outside [0, 24h)"},
		{name: "negative_wider_than_int64", s: "-9223372036854775809", unit: time.Millisecond, errMsg: "outside [0, 24h)"},
		// A MILLIS column is an INT32, so the day's worth of ticks is parsed at that width.
		{name: "wider_than_int32_millis", s: "2147483648", unit: time.Millisecond, errMsg: "outside [0, 24h)"},
		// The same digits are a legal 35m48s for a MICROS column, which is an INT64.
		{name: "wider_than_int32_micros", s: "2147483648", unit: time.Microsecond, expected: 2147483648},
		{name: "empty", s: "", unit: time.Millisecond, errMsg: "parse TIME_MILLIS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTimeOfDay(tt.s, "TIME_MILLIS", tt.unit)
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestTimeOutOfRangeIsNotSilentlyRewritten covers the round trip an out-of-range TIME used
// to corrupt: the reader rendered 25 hours as "25:00:00.000", and writing that string back
// stored 25 instead of reporting it.
func TestTimeOutOfRangeIsNotSilentlyRewritten(t *testing.T) {
	millisCT := parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS)
	microsCT := parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS)
	int32PT := parquet.TypePtr(parquet.Type_INT32)
	int64PT := parquet.TypePtr(parquet.Type_INT64)

	tests := []struct {
		name     string
		rendered string
		pT       *parquet.Type
		cT       *parquet.ConvertedType
	}{
		{name: "millis_negative", rendered: TIME_MILLISToTimeFormat(-1000), pT: int32PT, cT: millisCT},
		{name: "millis_past_midnight", rendered: TIME_MILLISToTimeFormat(90000000), pT: int32PT, cT: millisCT},
		{name: "micros_negative", rendered: TIME_MICROSToTimeFormat(-1000000), pT: int64PT, cT: microsCT},
		{name: "micros_past_midnight", rendered: TIME_MICROSToTimeFormat(90000000000), pT: int64PT, cT: microsCT},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := StrToParquetType(tt.rendered, tt.pT, tt.cT, 0, 0)
			require.Error(t, err)
		})
	}
}

// TestTimeRoundTrip checks that every TIME value this library accepts comes back as itself.
func TestTimeRoundTrip(t *testing.T) {
	millisCT := parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS)
	microsCT := parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS)

	for _, millis := range []int32{0, 1, 45296789, 86399999} {
		v, err := StrToParquetType(TIME_MILLISToTimeFormat(millis), parquet.TypePtr(parquet.Type_INT32), millisCT, 0, 0)
		require.NoError(t, err)
		require.Equal(t, millis, v)
	}
	for _, micros := range []int64{0, 1, 45296789012, 86399999999} {
		v, err := StrToParquetType(TIME_MICROSToTimeFormat(micros), parquet.TypePtr(parquet.Type_INT64), microsCT, 0, 0)
		require.NoError(t, err)
		require.Equal(t, micros, v)
	}
}
