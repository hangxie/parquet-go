package types

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestINT96(t *testing.T) {
	t1 := time.Now().Truncate(time.Microsecond).UTC()
	s := TimeToINT96(t1)
	t2, err := INT96ToTime(s)
	require.NoError(t, err)
	require.True(t, t1.Equal(t2), "INT96 error: expected %v, got %v", t1, t2)
}

func TestINT96ToTime_InvalidInput(t *testing.T) {
	// Test that INT96ToTime returns error for invalid input
	_, err := INT96ToTime("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")

	_, err = INT96ToTime("short")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")
}

func TestINT96ToTime(t *testing.T) {
	// Test valid input
	t1 := time.Now().Truncate(time.Microsecond).UTC()
	s := TimeToINT96(t1)
	t2, err := INT96ToTime(s)
	require.NoError(t, err)
	require.True(t, t1.Equal(t2), "INT96 error: expected %v, got %v", t1, t2)

	// Test with empty string
	_, err = INT96ToTime("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")

	// Test with string too short
	_, err = INT96ToTime("short")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")

	// Test with exactly 11 bytes (one byte short)
	_, err = INT96ToTime("12345678901")
	require.Error(t, err)
	require.Contains(t, err.Error(), "got 11 bytes")

	// Test with exactly 12 bytes (valid)
	_, err = INT96ToTime("123456789012")
	require.NoError(t, err)
}

func TestParseINT96String(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		errMsg string
	}{
		{
			name:  "valid_timestamp",
			input: "2024-01-15T14:30:45.123Z",
		},
		{
			name:  "epoch",
			input: "1970-01-01T00:00:00Z",
		},
		{
			name:   "invalid_format",
			input:  "not a timestamp",
			errMsg: "cannot parse",
		},
		{
			name:  "wide_year",
			input: "10000-01-01T00:00:00Z",
		},
		{
			name:   "unpadded_year",
			input:  "999-01-01T00:00:00Z",
			errMsg: "cannot parse",
		},
		{
			name:  "negative_year",
			input: "-4713-11-24T00:00:00.000000000Z",
		},
		{
			name:  "negative_year_with_leading_zero",
			input: "-0123-01-01T00:00:00Z",
		},
		{
			name:   "one_digit_negative_year",
			input:  "-1-01-01T00:00:00Z",
			errMsg: "cannot parse",
		},
		{
			name:   "two_digit_negative_year",
			input:  "-12-01-01T00:00:00Z",
			errMsg: "cannot parse",
		},
		{
			name:   "three_digit_negative_year",
			input:  "-123-01-01T00:00:00Z",
			errMsg: "cannot parse",
		},
		{
			// Year 10000 is divisible by 400, so this day exists; the stand-in year the
			// parser vets the rest of the string against must not decide that by itself.
			name:  "leap_day_in_leap_year",
			input: "10000-02-29T00:00:00Z",
		},
		{
			name:   "leap_day_in_common_year",
			input:  "10001-02-29T00:00:00Z",
			errMsg: "day 29 is out of range for February of year 10001",
		},
		{
			name:   "leap_day_in_negative_common_year",
			input:  "-0001-02-29T00:00:00Z",
			errMsg: "day 29 is out of range for February of year -1",
		},
		{
			name:  "last_representable_day",
			input: "5874898-06-03T00:00:00.000000000Z",
		},
		{
			name:   "past_last_representable_day",
			input:  "5874898-06-04T00:00:00.000000000Z",
			errMsg: `is outside the range an INT96 can hold`,
		},
		{
			name:  "first_representable_day",
			input: "-5884323-05-15T00:00:00.000000000Z",
		},
		{
			name:   "before_first_representable_day",
			input:  "-5884323-05-14T00:00:00.000000000Z",
			errMsg: `is outside the range an INT96 can hold`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseINT96String(tt.input)
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
				require.Len(t, result, 12)
			}
		})
	}
}

// TestINT96PhysicalRoundTrip covers INT96 values this library did not produce itself: a file
// from another writer can carry any Julian day and any nanosecond within the day.
func TestINT96PhysicalRoundTrip(t *testing.T) {
	tests := map[string]struct {
		value    string
		rendered string
	}{
		// Julian day 0 is 4713 BC, more than 292 years from the Unix epoch, so the whole
		// value does not fit a nanosecond offset from it.
		"julian_day_zero": {
			rawINT96(0, 0), "-4713-11-24T00:00:00.000000000Z",
		},
		"julian_day_one": {
			rawINT96(0, 1), "-4713-11-25T00:00:00.000000000Z",
		},
		"epoch": {
			rawINT96(0, uint32(JULIAN_DAY_OF_EPOCH)), "1970-01-01T00:00:00.000000000Z",
		},
		// Sub-microsecond nanoseconds are the part the day field cannot hold.
		"nanosecond_precision": {
			rawINT96(123456789, uint32(JULIAN_DAY_OF_EPOCH)), "1970-01-01T00:00:00.123456789Z",
		},
		"last_nanosecond_of_day": {
			rawINT96(uint64(24*time.Hour.Nanoseconds()-1), uint32(JULIAN_DAY_OF_EPOCH)), "1970-01-01T23:59:59.999999999Z",
		},
		"far_future_day": {
			rawINT96(0, math.MaxInt32), "5874898-06-03T00:00:00.000000000Z",
		},
	}

	se := parquet.NewSchemaElement()
	se.Type = parquet.TypePtr(parquet.Type_INT96)

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rendered := ConvertToJSONType(tc.value, se)
			require.Equal(t, tc.rendered, rendered)

			back, err := StrToParquetType(tc.rendered, parquet.TypePtr(parquet.Type_INT96), nil, 0, 0)
			require.NoError(t, err)
			require.Equal(t, tc.value, back)
		})
	}
}

// rawINT96 builds an INT96 value from its wire fields: nanoseconds within the day, then the
// Julian day.
func rawINT96(nanos uint64, days uint32) string {
	b := make([]byte, int96ByteLength)
	binary.LittleEndian.PutUint64(b[0:8], nanos)
	binary.LittleEndian.PutUint32(b[8:12], days)
	return string(b)
}

// TestParseINT96StringOffset covers a wide year carrying an explicit UTC offset. time.Parse
// hands back time.Local whenever the offset matches the local zone's, so rebuilding the
// timestamp in that location would apply the zone's rules for the replacement year instead of
// the offset the string spelled out.
func TestParseINT96StringOffset(t *testing.T) {
	// March 20 is inside US daylight saving time, where New York runs at -04:00 rather than
	// the -05:00 the input declares.
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("no time zone database: %v", err)
	}
	saved := time.Local
	time.Local = newYork
	t.Cleanup(func() { time.Local = saved })

	withOffset, err := ParseINT96String("10000-03-20T12:00:00-05:00")
	require.NoError(t, err)

	asUTC, err := ParseINT96String("10000-03-20T17:00:00Z")
	require.NoError(t, err)
	require.Equal(t, asUTC, withOffset)
}

// TestStrToINT96 covers the write path's choice between the timestamp form and the legacy
// integer form. A timestamp the parser rejects must fail the write rather than being stored as
// the digits it happens to start with.
func TestStrToINT96(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected string
		errMsg   string
	}{
		"timestamp": {
			"1970-01-01T00:00:00.123456789Z", rawINT96(123456789, uint32(JULIAN_DAY_OF_EPOCH)), "",
		},
		"bare_integer": {
			"0", string(make([]byte, int96ByteLength)), "",
		},
		"negative_integer": {
			"-1", "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", "",
		},
		"largest_96_bit_integer": {
			"79228162514264337593543950335",
			StrIntToBinary("79228162514264337593543950335", "LittleEndian", int96ByteLength, true), "",
		},
		"integer_too_wide": {
			"79228162514264337593543950336", "", "exceeds 12 bytes",
		},
		"negative_integer_too_wide": {
			"-79228162514264337593543950336", "", "exceeds 12 bytes",
		},
		"day_past_the_range": {
			"5874898-06-04T00:00:00.000000000Z", "", "is outside the range an INT96 can hold",
		},
		"day_not_in_its_month": {
			"10001-02-29T00:00:00Z", "", "day 29 is out of range for February of year 10001",
		},
		"not_a_timestamp": {
			"not a timestamp", "", "parse INT96 timestamp",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			res, err := StrToParquetType(tc.input, parquet.TypePtr(parquet.Type_INT96), nil, 0, 0)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, res)
		})
	}
}

// TestINT96ToTimeAcceptsEveryTwelveByteValue pins what convertINT96Value relies on when it
// drops INT96ToTime's error: the only value INT96ToTime rejects is one shorter than the
// width the format fixes, which convertINT96Value has already rejected itself.
func TestINT96ToTimeAcceptsEveryTwelveByteValue(t *testing.T) {
	for _, b := range [][]byte{
		make([]byte, int96ByteLength),
		bytes.Repeat([]byte{0xff}, int96ByteLength),
		bytes.Repeat([]byte{0x80}, int96ByteLength),
		append(bytes.Repeat([]byte{0xff}, 8), 0x00, 0x00, 0x00, 0x80),
	} {
		_, err := INT96ToTime(string(b))
		require.NoError(t, err, "% x", b)
	}

	_, err := INT96ToTime(string(make([]byte, int96ByteLength-1)))
	require.Error(t, err)
}
