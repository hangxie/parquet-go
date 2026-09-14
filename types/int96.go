package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// TimeToINT96 converts a time to its INT96 form. A time outside the Julian day range the
// column can hold wraps; ParseINT96String rejects those rather than wrapping them.
func TimeToINT96(t time.Time) string {
	days, nanos := toJulianDay(t)

	bs1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs1, uint64(nanos))

	bs2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs2, uint32(int32(days)))

	bs := append(bs1, bs2...)
	return string(bs)
}

// int96ByteLength is the required length for INT96 binary data
const int96ByteLength = 12

// INT96ToTime converts INT96 binary data to time.Time with error handling.
// Returns an error if the input is shorter than 12 bytes.
func INT96ToTime(int96 string) (time.Time, error) {
	if len(int96) < int96ByteLength {
		return time.Time{}, fmt.Errorf("INT96 data too short: got %d bytes, need %d", len(int96), int96ByteLength)
	}
	nanos := binary.LittleEndian.Uint64([]byte(int96[:8]))
	days := binary.LittleEndian.Uint32([]byte(int96[8:12]))

	return fromJulianDay(int32(days), int64(nanos)), nil
}

// ParseINT96String parses an ISO8601 timestamp string and returns INT96 binary representation
func ParseINT96String(s string) (string, error) {
	t, err := parseWideYearRFC3339(s)
	if err != nil {
		return "", fmt.Errorf("parse INT96 timestamp %q: %w", s, err)
	}
	// The column stores the day in 32 bits. Checking here rather than letting TimeToINT96
	// wrap keeps a day past the end of the range from being stored as an unrelated date.
	if days, _ := toJulianDay(t); days < math.MinInt32 || days > math.MaxInt32 {
		return "", fmt.Errorf("INT96 timestamp %q is outside the range an INT96 can hold", s)
	}
	return TimeToINT96(t.UTC()), nil
}

// parseWideYearRFC3339 parses an RFC3339 timestamp whose year may be negative or wider than
// four digits. The INT96 Julian day spans 4713 BC to year 5874898, so the read path renders
// years time.Parse refuses to read back; without this the write path fell through to the
// integer form and stored an unrelated value.
func parseWideYearRFC3339(s string) (time.Time, error) {
	if len(s) == 0 {
		return time.Parse(time.RFC3339Nano, s)
	}
	// The year ends at the first "-" after any leading sign.
	sep := strings.IndexByte(s[1:], '-') + 1
	if sep <= 0 || sep == 4 {
		return time.Parse(time.RFC3339Nano, s)
	}
	year, err := strconv.Atoi(s[:sep])
	if err != nil {
		// Not a year at all; the standard parser words the complaint.
		return time.Parse(time.RFC3339Nano, s)
	}

	// A leap-year stand-in keeps the standard parser checking the month, day, time and zone,
	// including 02-29, before the real year goes back in.
	t, err := time.Parse(time.RFC3339Nano, "2000-"+s[sep+1:])
	if err != nil {
		return time.Time{}, err
	}

	// Rebuilding in UTC and shifting by the parsed offset, rather than reusing the parsed
	// location: time.Parse hands back time.Local whenever the offset matches it, and the
	// zone's rules for the replacement year are not the ones the offset spelled out.
	_, offset := t.Zone()
	wall := time.Date(year, t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	// February 29 is the one field the stand-in year cannot vet, and time.Date normalizes
	// rather than rejecting it.
	if wall.Year() != year || wall.Month() != t.Month() || wall.Day() != t.Day() {
		return time.Time{}, fmt.Errorf("day %d is out of range for %s of year %d", t.Day(), t.Month(), year)
	}
	return wall.Add(-time.Duration(offset) * time.Second), nil
}

// convertINT96Value handles INT96 to datetime string conversion.
func convertINT96Value(val any) any {
	if v, ok := val.(string); ok {
		t, err := INT96ToTime(v)
		if err != nil {
			return val
		}
		return t.Format("2006-01-02T15:04:05.000000000Z")
	}
	return val
}
