package types

import (
	"encoding/binary"
	"fmt"
	"time"
)

func TimeToINT96(t time.Time) string {
	days, nanos := toJulianDay(t)

	bs1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs1, uint64(nanos))

	bs2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs2, uint32(days))

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
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return "", err
	}
	return TimeToINT96(t.UTC()), nil
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
