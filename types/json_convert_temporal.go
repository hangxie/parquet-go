package types

import (
	"fmt"
	"time"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// ConvertTimestampValue handles timestamp conversion to ISO8601 format
func ConvertTimestampValue(val any, convertedType parquet.ConvertedType) any {
	if val == nil {
		return nil
	}

	switch convertedType {
	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		if v, ok := val.(int64); ok {
			return TIMESTAMP_MILLISToISO8601(v, true) // Assume UTC adjusted
		}
	case parquet.ConvertedType_TIMESTAMP_MICROS:
		if v, ok := val.(int64); ok {
			return TIMESTAMP_MICROSToISO8601(v, true) // Assume UTC adjusted
		}
	}

	return val
}

// convertTimestampLogicalValue handles timestamp LogicalType conversion to ISO8601 format
func convertTimestampLogicalValue(val any, timestamp *parquet.TimestampType) any {
	if val == nil || timestamp == nil {
		return nil
	}

	v, ok := val.(int64)
	if !ok {
		return val
	}

	// Determine if timestamp is UTC adjusted
	adjustedToUTC := timestamp.IsAdjustedToUTC

	// Handle different timestamp units
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

	// Default to milliseconds if unit is not specified
	return TIMESTAMP_MILLISToISO8601(v, adjustedToUTC)
}

// ConvertTimeLogicalValue handles time LogicalType conversion to time format
func ConvertTimeLogicalValue(val any, timeType *parquet.TimeType) any {
	if val == nil || timeType == nil {
		return val
	}

	// Handle different time units
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
			// NANOS would be stored as int64, but we can treat it similarly to micros
			if v, ok := val.(int64); ok {
				// Convert nanos to the same format but preserve nanosecond precision
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

// ConvertDateLogicalValue handles DATE LogicalType conversion to date format "2006-01-02"
func ConvertDateLogicalValue(val any) any {
	if val == nil {
		return val
	}

	// DATE is stored as int32 days since Unix epoch (1970-01-01)
	if v, ok := val.(int32); ok {
		// Convert days since epoch to time.Time
		epochDate := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		resultDate := epochDate.AddDate(0, 0, int(v))
		return resultDate.Format("2006-01-02")
	}

	return val
}
