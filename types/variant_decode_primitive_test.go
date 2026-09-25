package types

import (
	"math/big"
	"testing"
)

func TestFormatDecimal(t *testing.T) {
	tests := []struct {
		unscaled int64
		scale    int
		expected string
	}{
		{12345, 2, "123.45"},
		{-12345, 2, "-123.45"},
		{12345, 0, "12345"},
		{5, 3, "0.005"},
		{100, 2, "1"},       // trailing zeros removed
		{1000, 3, "1"},      // trailing zeros removed
		{12300, 2, "123"},   // trailing zeros removed
		{12340, 2, "123.4"}, // partial trailing zeros
		{-5, 3, "-0.005"},
	}

	for _, tc := range tests {
		result := formatDecimal(tc.unscaled, tc.scale)
		if result != tc.expected {
			t.Errorf("formatDecimal(%d, %d) = %q, expected %q", tc.unscaled, tc.scale, result, tc.expected)
		}
	}
}

func TestFormatDecimal128(t *testing.T) {
	tests := []struct {
		unscaled string // big.Int string representation
		scale    int
		expected string
	}{
		{"12345", 2, "123.45"},
		{"-12345", 2, "-123.45"},
		{"12345", 0, "12345"},
		{"5", 3, "0.005"},
		{"-5", 3, "-0.005"},
		{"100", 2, "1"},
	}

	for _, tc := range tests {
		unscaled := new(big.Int)
		unscaled.SetString(tc.unscaled, 10)
		result := formatDecimal128(unscaled, tc.scale)
		if result != tc.expected {
			t.Errorf("formatDecimal128(%s, %d) = %q, expected %q", tc.unscaled, tc.scale, result, tc.expected)
		}
	}
}

// Additional error path tests for decodePrimitiveValue

func TestDecodePrimitiveValue_TruncatedInt8(t *testing.T) {
	// Int8 with no data after header
	data := []byte{0x0C} // primitive_type=3 (int8), but no value byte
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated int8")
	}
}

func TestDecodePrimitiveValue_TruncatedInt16(t *testing.T) {
	data := []byte{0x10, 0x01} // primitive_type=4 (int16), but only 1 byte
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated int16")
	}
}

func TestDecodePrimitiveValue_TruncatedInt32(t *testing.T) {
	data := []byte{0x14, 0x01, 0x02} // primitive_type=5 (int32), but only 2 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated int32")
	}
}

func TestDecodePrimitiveValue_TruncatedInt64(t *testing.T) {
	data := []byte{0x18, 0x01, 0x02, 0x03, 0x04} // primitive_type=6 (int64), but only 4 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated int64")
	}
}

func TestDecodePrimitiveValue_TruncatedDouble(t *testing.T) {
	data := []byte{0x1C, 0x01, 0x02, 0x03, 0x04} // primitive_type=7 (double), but only 4 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated double")
	}
}

func TestDecodePrimitiveValue_TruncatedFloat(t *testing.T) {
	data := []byte{0x38, 0x01, 0x02} // primitive_type=14 (float), but only 2 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated float")
	}
}

func TestDecodePrimitiveValue_TruncatedDate(t *testing.T) {
	data := []byte{0x2C, 0x01, 0x02} // primitive_type=11 (date), but only 2 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated date")
	}
}

func TestDecodePrimitiveValue_TruncatedTimestamp(t *testing.T) {
	data := []byte{0x30, 0x01, 0x02, 0x03, 0x04} // primitive_type=12 (timestamp), but only 4 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated timestamp")
	}
}

func TestDecodePrimitiveValue_TruncatedLongString(t *testing.T) {
	// Long string with length but truncated content
	data := []byte{0x40, 0x0A, 0x00, 0x00, 0x00, 'h', 'e', 'l'} // length=10, but only 3 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated long string")
	}
}

func TestDecodePrimitiveValue_TruncatedLongStringLength(t *testing.T) {
	// Long string with truncated length field
	data := []byte{0x40, 0x0A, 0x00} // only 2 bytes of length instead of 4
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated string length")
	}
}

func TestDecodePrimitiveValue_TruncatedBinary(t *testing.T) {
	// Binary with length but truncated content
	data := []byte{0x3C, 0x0A, 0x00, 0x00, 0x00, 0x01, 0x02} // length=10, but only 2 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated binary")
	}
}

func TestDecodePrimitiveValue_TruncatedBinaryLength(t *testing.T) {
	data := []byte{0x3C, 0x0A, 0x00} // only 2 bytes of length
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated binary length")
	}
}

func TestDecodePrimitiveValue_TruncatedUUID(t *testing.T) {
	data := []byte{0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08} // only 8 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated UUID")
	}
}

func TestDecodePrimitiveValue_TruncatedDecimal4(t *testing.T) {
	data := []byte{0x20, 0x02, 0x01, 0x02} // scale + only 2 bytes of value
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated decimal4")
	}
}

func TestDecodePrimitiveValue_TruncatedDecimal8(t *testing.T) {
	data := []byte{0x24, 0x02, 0x01, 0x02, 0x03, 0x04} // scale + only 4 bytes of value
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated decimal8")
	}
}

func TestDecodePrimitiveValue_TruncatedDecimal16(t *testing.T) {
	data := []byte{0x28, 0x02, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08} // scale + only 8 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated decimal16")
	}
}

// TestDecodePrimitiveTemporal_TruncatedTimestamp covers the not-enough-data path for timestamps.
// variantPrimitiveTimestampMicro = 12, value_metadata = (12<<2)|0 = 0x30.
func TestDecodePrimitiveTemporal_TruncatedTimestamp(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}
	// Only the header byte - 8 bytes of timestamp data are missing.
	data := []byte{0x30}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated timestamp")
	}
}

// TestDecodePrimitiveTemporal_TruncatedNanoTimestamp covers the nano-timestamp bounds check.
// variantPrimitiveTimestampNano = 18, value_metadata = (18<<2)|0 = 0x48.
func TestDecodePrimitiveTemporal_TruncatedNanoTimestamp(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}
	data := []byte{0x48}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated nano timestamp")
	}
}

// TestDecodePrimitiveTemporal_TruncatedTimeNTZ covers the variantPrimitiveTimeNTZ bounds check.
// variantPrimitiveTimeNTZ = 17, value_metadata = (17<<2)|0 = 0x44.
func TestDecodePrimitiveTemporal_TruncatedTimeNTZ(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}
	data := []byte{0x44}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated time-ntz value")
	}
}
