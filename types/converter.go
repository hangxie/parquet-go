package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

func TimeToTIME_MILLIS(t time.Time, adjustedToUTC bool) int64 {
	return TimeToTIME_MICROS(t, adjustedToUTC) / time.Millisecond.Microseconds()
}

func TimeToTIME_MICROS(t time.Time, adjustedToUTC bool) int64 {
	if adjustedToUTC {
		tu := t.UTC()
		h, m, s, ns := int64(tu.Hour()), int64(tu.Minute()), int64(tu.Second()), int64(tu.Nanosecond())
		nanos := h*time.Hour.Nanoseconds() + m*time.Minute.Nanoseconds() + s*time.Second.Nanoseconds() + ns*time.Nanosecond.Nanoseconds()
		return nanos / time.Microsecond.Nanoseconds()

	} else {
		h, m, s, ns := int64(t.Hour()), int64(t.Minute()), int64(t.Second()), int64(t.Nanosecond())
		nanos := h*time.Hour.Nanoseconds() + m*time.Minute.Nanoseconds() + s*time.Second.Nanoseconds() + ns*time.Nanosecond.Nanoseconds()
		return nanos / time.Microsecond.Nanoseconds()
	}
}

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
	} else {
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, t.Location())
		return t.Sub(epoch).Nanoseconds()
	}
}

func TIMESTAMP_NANOSToTime(nanos int64, adjustedToUTC bool) time.Time {
	if adjustedToUTC {
		return time.Unix(0, nanos).UTC()
	} else {
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
		t := epoch.Add(time.Nanosecond * time.Duration(nanos))
		return t
	}
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

func TimeToINT96(t time.Time) string {
	days, nanos := toJulianDay(t)

	bs1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs1, uint64(nanos))

	bs2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs2, uint32(days))

	bs := append(bs1, bs2...)
	return string(bs)
}

func INT96ToTime(int96 string) time.Time {
	nanos := binary.LittleEndian.Uint64([]byte(int96[:8]))
	days := binary.LittleEndian.Uint32([]byte(int96[8:]))

	return fromJulianDay(int32(days), int64(nanos))
}

func DECIMAL_INT_ToString(dec int64, precision, scale int) string {
	sign := ""
	if dec < 0 {
		sign = "-"
		dec = -dec
	}
	ans := strconv.FormatInt(dec, 10)
	if scale > 0 {
		if scale > len(ans) {
			ans = strings.Repeat("0", scale-(len(ans))+1) + ans
		}
		radixLoc := len(ans) - scale
		ans = ans[:radixLoc] + "." + ans[radixLoc:]
	}
	return sign + ans
}

func DECIMAL_BYTE_ARRAY_ToString(dec []byte, precision, scale int) string {
	sign := ""
	if dec[0] > 0x7f {
		sign = "-"
		for i := range dec {
			dec[i] = dec[i] ^ 0xff
		}
	}
	a := new(big.Int)
	a.SetBytes(dec)
	if sign == "-" {
		a = a.Add(a, big.NewInt(1))
	}
	sa := a.Text(10)

	if scale > 0 {
		ln := len(sa)
		if ln < scale+1 {
			sa = strings.Repeat("0", scale+1-ln) + sa
			ln = scale + 1
		}
		sa = sa[:ln-scale] + "." + sa[ln-scale:]
	}
	return sign + sa
}

func IntervalToString(interval []byte) string {
	if len(interval) != 12 {
		return ""
	}

	months := binary.LittleEndian.Uint32(interval[0:4])
	days := binary.LittleEndian.Uint32(interval[4:8])
	milliseconds := binary.LittleEndian.Uint32(interval[8:12])

	// Convert milliseconds to seconds with decimals
	seconds := float64(milliseconds) / 1000.0

	// Format as "XX mon XX day XX.xxx sec"
	var parts []string
	if months > 0 {
		parts = append(parts, fmt.Sprintf("%d mon", months))
	}
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%d day", days))
	}
	if seconds > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%.3f sec", seconds))
	}

	return strings.Join(parts, " ")
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

// ParseDateString parses a date string in format "2006-01-02" and returns days since Unix epoch
func ParseDateString(s string) (int32, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return 0, err
	}
	epochDate := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	days := int32(t.Sub(epochDate).Hours() / 24)
	return days, nil
}

// ParseTimeString parses a time string in format "HH:MM:SS" or "HH:MM:SS.sssssssss"
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

// ParseINT96String parses an ISO8601 timestamp string and returns INT96 binary representation
func ParseINT96String(s string) (string, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return "", err
	}
	return TimeToINT96(t.UTC()), nil
}

// ParseIntervalString parses an interval string like "2 mon 3 day 4.500 sec" and returns 12-byte binary
func ParseIntervalString(s string) (string, error) {
	var months, days uint32
	var seconds float64

	s = strings.TrimSpace(s)
	if s == "" {
		// Return zero interval
		result := make([]byte, 12)
		return string(result), nil
	}

	parts := strings.Fields(s)
	for i := 0; i < len(parts); i += 2 {
		if i+1 >= len(parts) {
			return "", fmt.Errorf("invalid interval format: %s", s)
		}

		value := parts[i]
		unit := strings.ToLower(parts[i+1])

		switch {
		case strings.HasPrefix(unit, "mon"):
			var v uint32
			if _, err := fmt.Sscanf(value, "%d", &v); err != nil {
				return "", fmt.Errorf("invalid months value: %s", value)
			}
			months = v
		case strings.HasPrefix(unit, "day"):
			var v uint32
			if _, err := fmt.Sscanf(value, "%d", &v); err != nil {
				return "", fmt.Errorf("invalid days value: %s", value)
			}
			days = v
		case strings.HasPrefix(unit, "sec"):
			var v float64
			if _, err := fmt.Sscanf(value, "%f", &v); err != nil {
				return "", fmt.Errorf("invalid seconds value: %s", value)
			}
			seconds = v
		default:
			return "", fmt.Errorf("unknown interval unit: %s", unit)
		}
	}

	// Convert to binary: months (4 bytes) + days (4 bytes) + milliseconds (4 bytes)
	result := make([]byte, 12)
	binary.LittleEndian.PutUint32(result[0:4], months)
	binary.LittleEndian.PutUint32(result[4:8], days)
	binary.LittleEndian.PutUint32(result[8:12], uint32(seconds*1000))

	return string(result), nil
}

// ParseFloat16String parses a float string and returns 2-byte IEEE 754 half-precision binary
func ParseFloat16String(s string) (string, error) {
	var f float64
	if _, err := fmt.Sscanf(s, "%f", &f); err != nil {
		return "", fmt.Errorf("invalid float16 value: %s", s)
	}

	// Convert float64 to float16 (IEEE 754 half-precision)
	f32 := float32(f)
	bits := math.Float32bits(f32)

	// Extract components from float32
	sign := (bits >> 31) & 1
	exp := int((bits >> 23) & 0xFF)
	frac := bits & 0x7FFFFF

	var f16 uint16

	if exp == 0xFF {
		// Inf or NaN
		if frac != 0 {
			// NaN
			f16 = uint16((sign << 15) | 0x7C00 | (frac >> 13))
		} else {
			// Inf
			f16 = uint16((sign << 15) | 0x7C00)
		}
	} else if exp == 0 {
		// Zero or subnormal
		f16 = uint16(sign << 15)
	} else {
		// Normalized number
		newExp := exp - 127 + 15 // Rebias from float32 to float16

		if newExp >= 31 {
			// Overflow to infinity
			f16 = uint16((sign << 15) | 0x7C00)
		} else if newExp <= 0 {
			// Underflow to zero or subnormal
			if newExp < -10 {
				f16 = uint16(sign << 15)
			} else {
				// Subnormal
				frac = (frac | 0x800000) >> uint(1-newExp)
				f16 = uint16((sign << 15) | (frac >> 13))
			}
		} else {
			// Normal number
			f16 = uint16((sign << 15) | (uint32(newExp) << 10) | (frac >> 13))
		}
	}

	// Return as little-endian 2-byte string (Parquet uses little-endian for FLOAT16)
	result := make([]byte, 2)
	binary.LittleEndian.PutUint16(result, f16)
	return string(result), nil
}
