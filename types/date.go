package types

import "time"

// ParseDateString parses a date string in format "2006-01-02" and returns days since Unix epoch.
func ParseDateString(s string) (int32, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return 0, err
	}
	epochDate := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	days := int32(t.Sub(epochDate).Hours() / 24)
	return days, nil
}

// ConvertDateLogicalValue handles DATE LogicalType conversion to date format "2006-01-02".
func ConvertDateLogicalValue(val any) any {
	if val == nil {
		return val
	}

	if v, ok := val.(int32); ok {
		epochDate := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		resultDate := epochDate.AddDate(0, 0, int(v))
		return resultDate.Format("2006-01-02")
	}

	return val
}
