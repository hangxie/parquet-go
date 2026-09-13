package types

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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

// formatTimeOfDay renders ticks since midnight, where perSecond ticks make a second and
// fracDigits is the width of the fractional field.
func formatTimeOfDay(ticks int64, perSecond uint64, fracDigits int) string {
	// A TIME outside [0, 24h) is not writable through this library, but a file from another
	// writer can carry one: keep the sign in front of the whole value and let the hour field
	// grow past 24, so the string never reads as a different time than the one stored.
	sign, mag := "", uint64(ticks)
	if ticks < 0 {
		// negating the magnitude rather than the signed value keeps math.MinInt64 in range
		sign, mag = "-", -mag
	}

	seconds := mag / perSecond
	return fmt.Sprintf("%s%02d:%02d:%02d.%0*d", sign, seconds/3600, seconds/60%60, seconds%60, fracDigits, mag%perSecond)
}

func TIME_MILLISToTimeFormat(millis int32) string {
	return formatTimeOfDay(int64(millis), uint64(time.Second/time.Millisecond), 3)
}

func TIME_MICROSToTimeFormat(micros int64) string {
	return formatTimeOfDay(micros, uint64(time.Second/time.Microsecond), 6)
}

// timeUnitOf reports the tick size and spelling of a TIME column's unit.
func timeUnitOf(t *parquet.TimeType) (time.Duration, string, bool) {
	if t == nil || t.Unit == nil {
		return 0, "", false
	}
	switch {
	case t.Unit.IsSetMILLIS():
		return time.Millisecond, "TIME_MILLIS", true
	case t.Unit.IsSetMICROS():
		return time.Microsecond, "TIME_MICROS", true
	case t.Unit.IsSetNANOS():
		return time.Nanosecond, "TIME_NANOS", true
	}
	return 0, "", false
}

// timeTicksPerDay is the number of unit-sized ticks in the 24 hours a TIME value spans.
func timeTicksPerDay(unit time.Duration) int64 {
	return int64(24 * time.Hour / unit)
}

// parseTimeOfDay scans a TIME value written as either a clock string or a bare count of
// unit-sized ticks since midnight, rejecting anything outside the [0, 24h) the spec allows.
func parseTimeOfDay(s, typeName string, unit time.Duration) (int64, error) {
	// ParseTimeString only accepts hours 00-23, so a value it returns is always in range.
	nanos, parseErr := ParseTimeString(s)
	if parseErr == nil {
		return nanos / int64(unit), nil
	}

	// A MILLIS column stores its ticks in an INT32 and the whole day fits there, so parse at
	// the width the column actually has: the narrowing the callers do is then safe by
	// construction rather than by the range check below.
	bitSize := 64
	if unit == time.Millisecond {
		bitSize = 32
	}

	// Sscanf used to take the leading digits of whatever it was handed, so the reader's own
	// "25:00:00.000" came back as 25 and "12abc" as 12; ParseInt refuses both.
	ticks, err := strconv.ParseInt(strings.TrimSpace(s), 10, bitSize)
	switch {
	case errors.Is(err, strconv.ErrRange):
		// Too wide for the column, and so far past the end of the day that the range is the
		// useful thing to report rather than the scan failure.
		return 0, fmt.Errorf("%s %q is outside [0, 24h)", typeName, s)
	case err != nil:
		return 0, fmt.Errorf("parse %s %q: %w", typeName, s, parseErr)
	case ticks < 0 || ticks >= timeTicksPerDay(unit):
		return 0, fmt.Errorf("%s %q is outside [0, 24h)", typeName, s)
	}
	return ticks, nil
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
				return formatTimeOfDay(v, uint64(time.Second/time.Nanosecond), 9)
			}
		}
	}

	return val
}

func strToTimeLogical(s string, t *parquet.TimeType) (any, error) {
	unit, typeName, ok := timeUnitOf(t)
	if !ok {
		return nil, fmt.Errorf("time unit not set")
	}
	v, err := parseTimeOfDay(s, typeName, unit)
	if unit == time.Millisecond {
		return int32(v), err
	}
	return v, err
}
