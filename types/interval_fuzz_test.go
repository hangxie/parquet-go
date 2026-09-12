package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzParseIntervalString(f *testing.F) {
	f.Add("")
	f.Add("1 mon")
	f.Add("2 day")
	f.Add("3.500 sec")
	f.Add("1 mon 2 day 3.500 sec")
	f.Add("invalid input")
	f.Add("0 day 0 day")

	f.Fuzz(func(t *testing.T, s string) {
		parsed, err := ParseIntervalString(s)
		if err != nil {
			return
		}
		// whatever a valid interval renders as must parse back to the same 12 bytes
		reparsed, err := ParseIntervalString(IntervalToString([]byte(parsed)))
		require.NoError(t, err)
		require.Equal(t, parsed, reparsed)
	})
}
