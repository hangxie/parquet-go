package types

import (
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
