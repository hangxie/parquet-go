package types

import (
	"fmt"
	"time"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TimeToTIMESTAMP_MILLIS(t time.Time, adjustedToUTC bool) int64 {
	return TimeToTIMESTAMP_MICROS(t, adjustedToUTC) / time.Millisecond.Microseconds()
}

func TIMESTAMP_MILLISToTime(millis int64, adjustedToUTC bool) time.Time {
	return TIMESTAMP_MICROSToTime(millis*time.Millisecond.Microseconds(), adjustedToUTC)
}

func TimeToTIMESTAMP_MICROS(t time.Time, adjustedToUTC bool) int64 {
	return TimeToTIMESTAMP_NANOS(t, adjustedToUTC) / time.Microsecond.Nanoseconds()
}

func TIMESTAMP_MICROSToTime(micros int64, adjustedToUTC bool) time.Time {
	return TIMESTAMP_NANOSToTime(micros*time.Microsecond.Nanoseconds(), adjustedToUTC)
}

func TimeToTIMESTAMP_NANOS(t time.Time, adjustedToUTC bool) int64 {
	if adjustedToUTC {
		return t.UnixNano()
	}
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, t.Location())
	return t.Sub(epoch).Nanoseconds()
}

func TIMESTAMP_NANOSToTime(nanos int64, adjustedToUTC bool) time.Time {
	if adjustedToUTC {
		return time.Unix(0, nanos).UTC()
	}
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
	t := epoch.Add(time.Nanosecond * time.Duration(nanos))
	return t
}

func TIMESTAMP_MILLISToISO8601(millis int64, adjustedToUTC bool) string {
	t := TIMESTAMP_MILLISToTime(millis, adjustedToUTC)
	return t.Format("2006-01-02T15:04:05.000Z")
}

func TIMESTAMP_MICROSToISO8601(micros int64, adjustedToUTC bool) string {
	t := TIMESTAMP_MICROSToTime(micros, adjustedToUTC)
	return t.Format("2006-01-02T15:04:05.000000Z")
}

func TIMESTAMP_NANOSToISO8601(nanos int64, adjustedToUTC bool) string {
	t := TIMESTAMP_NANOSToTime(nanos, adjustedToUTC)
	return t.Format("2006-01-02T15:04:05.000000000Z")
}

// ConvertTimestampValue handles timestamp conversion to ISO8601 format.
func ConvertTimestampValue(val any, convertedType parquet.ConvertedType) any {
	if val == nil {
		return nil
	}

	switch convertedType {
	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		if v, ok := val.(int64); ok {
			return TIMESTAMP_MILLISToISO8601(v, true)
		}
	case parquet.ConvertedType_TIMESTAMP_MICROS:
		if v, ok := val.(int64); ok {
			return TIMESTAMP_MICROSToISO8601(v, true)
		}
	}

	return val
}

func convertTimestampLogicalValue(val any, timestamp *parquet.TimestampType) any {
	if val == nil || timestamp == nil {
		return nil
	}

	v, ok := val.(int64)
	if !ok {
		return val
	}

	adjustedToUTC := timestamp.IsAdjustedToUTC
	if timestamp.Unit != nil {
		if timestamp.Unit.IsSetMILLIS() {
			return TIMESTAMP_MILLISToISO8601(v, adjustedToUTC)
		}
		if timestamp.Unit.IsSetMICROS() {
			return TIMESTAMP_MICROSToISO8601(v, adjustedToUTC)
		}
		if timestamp.Unit.IsSetNANOS() {
			return TIMESTAMP_NANOSToISO8601(v, adjustedToUTC)
		}
	}

	return TIMESTAMP_MILLISToISO8601(v, adjustedToUTC)
}

// hasTimestampUnit reports whether the annotation names a unit this package can read. A
// thrift union naming an unknown member decodes with no field set, saying as little as nil.
func hasTimestampUnit(ts *parquet.TimestampType) bool {
	return ts != nil && ts.Unit != nil &&
		(ts.Unit.IsSetMILLIS() || ts.Unit.IsSetMICROS() || ts.Unit.IsSetNANOS())
}

// timestampLabel names a TIMESTAMP by its unit, so both spellings report alike.
func timestampLabel(ts *parquet.TimestampType) string {
	switch {
	case ts.Unit.IsSetNANOS():
		return "TIMESTAMP_NANOS"
	case ts.Unit.IsSetMICROS():
		return "TIMESTAMP_MICROS"
	case ts.Unit.IsSetMILLIS():
		return "TIMESTAMP_MILLIS"
	}
	return "TIMESTAMP"
}

func strToTimestampLogical(s string, ts *parquet.TimestampType) (any, error) {
	if hasTimestampUnit(ts) {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			switch {
			case ts.Unit.IsSetNANOS():
				return t.UnixNano(), nil
			case ts.Unit.IsSetMICROS():
				return t.UnixNano() / int64(time.Microsecond), nil
			case ts.Unit.IsSetMILLIS():
				return t.UnixNano() / int64(time.Millisecond), nil
			}
		}
		return strToTickCount(s, timestampLabel(ts))
	}
	return nil, fmt.Errorf("timestamp unit not set")
}
