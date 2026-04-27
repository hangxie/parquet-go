package types

import (
	"fmt"
	"time"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TimeToTIME_MILLIS(t time.Time, adjustedToUTC bool) int64 {
	return TimeToTIME_MICROS(t, adjustedToUTC) / time.Millisecond.Microseconds()
}

func TimeToTIME_MICROS(t time.Time, adjustedToUTC bool) int64 {
	if adjustedToUTC {
		t = t.UTC()
	}

	h, m, s, ns := int64(t.Hour()), int64(t.Minute()), int64(t.Second()), int64(t.Nanosecond())
	nanos := h*time.Hour.Nanoseconds() + m*time.Minute.Nanoseconds() + s*time.Second.Nanoseconds() + ns*time.Nanosecond.Nanoseconds()
	return nanos / time.Microsecond.Nanoseconds()
}

// From Spark
// https://github.com/apache/spark/blob/b9f2f78de59758d1932c1573338539e485a01112/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala#L47
const (
	JULIAN_DAY_OF_EPOCH int64 = 2440588
	MICROS_PER_DAY      int64 = 3600 * 24 * 1000 * 1000
)

// From Spark
// https://github.com/apache/spark/blob/b9f2f78de59758d1932c1573338539e485a01112/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala#L180
func toJulianDay(t time.Time) (int32, int64) {
	utc := t.UTC()
	nanos := utc.UnixNano()
	micros := nanos / time.Microsecond.Nanoseconds()

	julianUs := micros + JULIAN_DAY_OF_EPOCH*MICROS_PER_DAY
	days := int32(julianUs / MICROS_PER_DAY)
	us := (julianUs % MICROS_PER_DAY) * 1000
	return days, us
}

// From Spark
// https://github.com/apache/spark/blob/b9f2f78de59758d1932c1573338539e485a01112/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala#L170
func fromJulianDay(days int32, nanos int64) time.Time {
	nanos = ((int64(days)-JULIAN_DAY_OF_EPOCH)*MICROS_PER_DAY + nanos/1000) * 1000
	sec, nsec := nanos/time.Second.Nanoseconds(), nanos%time.Second.Nanoseconds()
	t := time.Unix(sec, nsec)
	return t.UTC()
}

func TIME_MILLISToTimeFormat(millis int32) string {
	totalNanos := int64(millis) * int64(time.Millisecond)
	hours := totalNanos / int64(time.Hour)
	totalNanos %= int64(time.Hour)
	minutes := totalNanos / int64(time.Minute)
	totalNanos %= int64(time.Minute)
	seconds := totalNanos / int64(time.Second)
	totalNanos %= int64(time.Second)
	nanos := totalNanos

	return fmt.Sprintf("%02d:%02d:%02d.%03d", hours, minutes, seconds, nanos/int64(time.Millisecond))
}

func TIME_MICROSToTimeFormat(micros int64) string {
	totalNanos := micros * int64(time.Microsecond)
	hours := totalNanos / int64(time.Hour)
	totalNanos %= int64(time.Hour)
	minutes := totalNanos / int64(time.Minute)
	totalNanos %= int64(time.Minute)
	seconds := totalNanos / int64(time.Second)
	totalNanos %= int64(time.Second)
	nanos := totalNanos

	return fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, nanos/int64(time.Microsecond))
}

// ParseTimeString parses a time string in format \"HH:MM:SS\" or \"HH:MM:SS.sssssssss\"
// and returns the value in nanoseconds
func ParseTimeString(s string) (int64, error) {
	t, err := time.Parse("15:04:05.000000000", s)
	if err != nil {
		t, err = time.Parse("15:04:05", s)
		if err != nil {
			return 0, fmt.Errorf("cannot parse time string: %s", s)
		}
	}
	h, m, sec := int64(t.Hour()), int64(t.Minute()), int64(t.Second())
	ns := int64(t.Nanosecond())
	return h*int64(time.Hour) + m*int64(time.Minute) + sec*int64(time.Second) + ns, nil
}

// ConvertTimeLogicalValue handles time LogicalType conversion to time format.
func ConvertTimeLogicalValue(val any, timeType *parquet.TimeType) any {
	if val == nil || timeType == nil {
		return val
	}

	if timeType.Unit != nil {
		if timeType.Unit.IsSetMILLIS() {
			if v, ok := val.(int32); ok {
				return TIME_MILLISToTimeFormat(v)
			}
		}
		if timeType.Unit.IsSetMICROS() {
			if v, ok := val.(int64); ok {
				return TIME_MICROSToTimeFormat(v)
			}
		}
		if timeType.Unit.IsSetNANOS() {
			if v, ok := val.(int64); ok {
				totalNanos := v
				hours := totalNanos / int64(time.Hour)
				totalNanos %= int64(time.Hour)
				minutes := totalNanos / int64(time.Minute)
				totalNanos %= int64(time.Minute)
				seconds := totalNanos / int64(time.Second)
				totalNanos %= int64(time.Second)
				nanos := totalNanos

				return fmt.Sprintf("%02d:%02d:%02d.%09d", hours, minutes, seconds, nanos)
			}
		}
	}

	return val
}

func strToTimeLogical(s string, t *parquet.TimeType) (any, error) {
	if t.Unit == nil {
		return nil, fmt.Errorf("time unit not set")
	}
	if nanos, err := ParseTimeString(s); err == nil {
		switch {
		case t.Unit.IsSetNANOS():
			return nanos, nil
		case t.Unit.IsSetMICROS():
			return nanos / int64(time.Microsecond), nil
		case t.Unit.IsSetMILLIS():
			return int32(nanos / int64(time.Millisecond)), nil
		}
	}
	if t.Unit.IsSetMILLIS() {
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err
	}
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
