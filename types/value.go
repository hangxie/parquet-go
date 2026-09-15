package types

import (
	"fmt"
)

// errNoPhysicalType reports a schema element with no physical type to scan into.
func errNoPhysicalType(s string) error {
	return fmt.Errorf("cannot scan %q without a physical type", s)
}
