package types

import (
	"errors"
	"fmt"
)

// ValueMode selects how a logical value is represented outside the parquet file.
type ValueMode int

const (
	// ValueModeInterpreted carries the canonical text of the logical type.
	ValueModeInterpreted ValueMode = iota
	// ValueModeRaw carries the physical value: base64 for byte-backed columns, the
	// underlying number otherwise. Text-annotated columns stay text in both modes.
	ValueModeRaw
)

// IsValid reports whether the mode is one this package defines.
func (m ValueMode) IsValid() bool {
	return m == ValueModeInterpreted || m == ValueModeRaw
}

// String returns the mode's name.
func (m ValueMode) String() string {
	switch m {
	case ValueModeInterpreted:
		return "interpreted"
	case ValueModeRaw:
		return "raw"
	default:
		return fmt.Sprintf("ValueMode(%d)", int(m))
	}
}

// ValueConfig holds the settings the value conversion paths share.
type ValueConfig struct {
	// Mode selects the interpreted (default) or raw representation.
	Mode ValueMode
	// EnforceUTF8 rejects invalid UTF-8 in text-annotated values; default is false.
	EnforceUTF8 bool
	// Geospatial configures GEOMETRY/GEOGRAPHY rendering; nil selects the defaults.
	Geospatial *GeospatialConfig
}

// ValueOption configures value conversion.
type ValueOption func(*ValueConfig)

// JSONTypeConfig is the former name of ValueConfig.
//
// Deprecated: use ValueConfig.
type JSONTypeConfig = ValueConfig

// JSONTypeOption is the former name of ValueOption.
//
// Deprecated: use ValueOption.
type JSONTypeOption = ValueOption

// NewValueConfig builds a ValueConfig with default values, modified by opts.
func NewValueConfig(opts ...ValueOption) *ValueConfig {
	cfg := resolveValueConfig(opts)
	return &cfg
}

// resolveValueConfig applies opts to a zero config, by value.
func resolveValueConfig(opts []ValueOption) ValueConfig {
	// An option is an opaque closure over a pointer, so building a config through one
	// heap-allocates it. This runs once per value; the usual case of none must not.
	if len(opts) == 0 {
		return ValueConfig{}
	}
	cfg := new(ValueConfig)
	for _, opt := range opts {
		opt(cfg)
	}
	return *cfg
}

// ErrUnsupportedValueMode reports a mode outside the two this package defines. The
// conversion helpers and the writer option both wrap it, so errors.Is matches either.
var ErrUnsupportedValueMode = errors.New("unsupported value mode")

// WithValueMode selects the interpreted or raw representation of logical values.
func WithValueMode(m ValueMode) ValueOption {
	return func(c *ValueConfig) { c.Mode = m }
}

// WithGeospatialConfig sets a custom GeospatialConfig for GEOMETRY/GEOGRAPHY rendering.
func WithGeospatialConfig(cfg *GeospatialConfig) ValueOption {
	return func(c *ValueConfig) { c.Geospatial = cfg }
}

// WithEnforceUTF8 enables UTF-8 validation for STRING, UTF8, JSON and ENUM values.
func WithEnforceUTF8(enabled bool) ValueOption {
	return func(c *ValueConfig) { c.EnforceUTF8 = enabled }
}
