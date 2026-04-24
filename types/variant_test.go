package types

import (
	"testing"
)

func TestDecodeVariantMetadata_Empty(t *testing.T) {
	meta, err := decodeVariantMetadata([]byte{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(meta.dictionary) != 0 {
		t.Errorf("expected empty dictionary, got %d entries", len(meta.dictionary))
	}
}

func TestDecodeVariantMetadata_SingleEntry(t *testing.T) {
	// Build metadata: version=1, sorted=0, offset_size=1 (offset_size_minus_one=0)
	// Header byte layout: version (bits 0-3) | sorted (bit 4) | offset_size_minus_one (bits 5-6)
	// 0x01 = version=1, sorted=0, offset_size=1
	// dict_size: 1
	// offsets: [0, 4]
	// bytes: "test"
	data := []byte{
		0x01,               // header: version=1, sorted=0, offset_size=1
		0x01,               // dict_size=1
		0x00,               // offset[0]=0
		0x04,               // offset[1]=4
		't', 'e', 's', 't', // "test"
	}

	meta, err := decodeVariantMetadata(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(meta.dictionary) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(meta.dictionary))
	}
	if meta.dictionary[0] != "test" {
		t.Errorf("expected 'test', got %q", meta.dictionary[0])
	}
}

func TestDecodeVariantMetadata_MultipleEntries(t *testing.T) {
	// Header byte layout: version (bits 0-3) | sorted (bit 4) | offset_size_minus_one (bits 5-6)
	// version=1, sorted=1, offset_size=1 (offset_size_minus_one=0)
	// 0x01 | 0x10 = 0x11
	data := []byte{
		0x11,          // header: version=1, sorted=1, offset_size=1
		0x03,          // dict_size=3
		0x00,          // offset[0]=0
		0x03,          // offset[1]=3
		0x06,          // offset[2]=6
		0x0b,          // offset[3]=11
		'f', 'o', 'o', // "foo"
		'b', 'a', 'r', // "bar"
		'h', 'e', 'l', 'l', 'o', // "hello"
	}

	meta, err := decodeVariantMetadata(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(meta.dictionary) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(meta.dictionary))
	}
	expected := []string{"foo", "bar", "hello"}
	for i, exp := range expected {
		if meta.dictionary[i] != exp {
			t.Errorf("dictionary[%d]: expected %q, got %q", i, exp, meta.dictionary[i])
		}
	}
	if !meta.sorted {
		t.Errorf("expected sorted=true")
	}
}

func TestDecodeVariantMetadata_InvalidVersion(t *testing.T) {
	// Version 2 (unsupported) - version is in bits 0-3
	// 0x02 = version=2
	data := []byte{0x02, 0x00, 0x00}
	_, err := decodeVariantMetadata(data)
	if err == nil {
		t.Error("expected error for unsupported version")
	}
}

func TestDecodeVariantMetadata_TooShort(t *testing.T) {
	// Header only, missing dict_size
	// 0x01 = version=1, offset_size=1
	data := []byte{0x01}
	_, err := decodeVariantMetadata(data)
	if err == nil {
		t.Error("expected error for too short metadata")
	}
}

func TestDecodeVariantMetadata_OffsetOutOfBounds(t *testing.T) {
	// Valid header but offsets point beyond data
	// 0x01 = version=1, offset_size=1
	data := []byte{0x01, 0x01, 0x00, 0xFF} // dict_size=1, offsets [0, 255] but no string data
	_, err := decodeVariantMetadata(data)
	if err == nil {
		t.Error("expected error for offset out of bounds")
	}
}

func TestDecodeMetadata_TruncatedOffsets(t *testing.T) {
	// Header + dict_size but not enough offsets
	// 0x01 = version=1, offset_size=1
	data := []byte{0x01, 0x05, 0x00} // dict_size=5, but only 1 offset
	_, err := decodeVariantMetadata(data)
	if err == nil {
		t.Error("expected error for truncated offsets")
	}
}

// Tests for shredded variant encoding functions
