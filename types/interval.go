package types

import (
	"encoding/binary"
	"fmt"
	"strings"
)

func IntervalToString(interval []byte) string {
	if len(interval) != 12 {
		return ""
	}

	months := binary.LittleEndian.Uint32(interval[0:4])
	days := binary.LittleEndian.Uint32(interval[4:8])
	milliseconds := binary.LittleEndian.Uint32(interval[8:12])

	// Convert milliseconds to seconds with decimals
	seconds := float64(milliseconds) / 1000.0

	// Format as \"XX mon XX day XX.xxx sec\"
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

// ParseIntervalString parses an interval string like \"2 mon 3 day 4.500 sec\" and returns 12-byte binary
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

// convertIntervalValue handles INTERVAL to formatted string conversion.
func convertIntervalValue(val any) any {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case []byte:
		if len(v) == 12 {
			return IntervalToString(v)
		}
	case string:
		if len(v) == 12 {
			return IntervalToString([]byte(v))
		}
	}

	return val
}
