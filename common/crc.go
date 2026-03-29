package common

import (
	"fmt"
	"hash/crc32"
)

// CRCMode controls CRC validation behavior when reading pages.
type CRCMode int

const (
	// CRCIgnore skips all CRC validation (default, backward compatible).
	CRCIgnore CRCMode = iota
	// CRCAuto validates CRC when present in page header, passes when absent.
	CRCAuto
	// CRCStrict requires CRC on every page and validates it.
	CRCStrict
)

// ValidatePageCRC validates a page's CRC32 checksum against page data.
// The CRC uses the IEEE polynomial (CRC-32/ISO 3309).
// Multiple byte slices are hashed incrementally without concatenation.
func ValidatePageCRC(hasCRC bool, expectedCRC int32, mode CRCMode, pageData ...[]byte) error {
	if mode == CRCIgnore {
		return nil
	}

	if !hasCRC {
		if mode == CRCStrict {
			return fmt.Errorf("page CRC required but not present")
		}
		return nil
	}

	h := crc32.NewIEEE()
	for _, d := range pageData {
		h.Write(d)
	}
	actual := int32(h.Sum32())
	if actual != expectedCRC {
		return fmt.Errorf("page CRC mismatch: expected 0x%08X, got 0x%08X",
			uint32(expectedCRC), uint32(actual))
	}
	return nil
}

// ComputePageCRC computes CRC32 (IEEE polynomial) over page data.
// Multiple byte slices are hashed incrementally without concatenation,
// consistent with ValidatePageCRC.
func ComputePageCRC(pageData ...[]byte) uint32 {
	h := crc32.NewIEEE()
	for _, d := range pageData {
		h.Write(d)
	}
	return h.Sum32()
}
