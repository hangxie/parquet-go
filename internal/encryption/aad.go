// Package encryption provides stateless AES-GCM and AES-CTR primitives for
// Parquet modular encryption, including module encoding/decoding, AAD
// construction, and the Parquet-specified module type constants. Callers are
// responsible for key selection, ordinal tracking, and wiring these primitives
// into the read and write paths.
package encryption

import "encoding/binary"

// ModuleType identifies an encrypted Parquet module for AAD suffix construction.
type ModuleType byte

const (
	ModuleFooter ModuleType = iota
	ModuleColumnMetaData
	ModuleDataPage
	ModuleDictionaryPage
	ModuleDataPageHeader
	ModuleDictionaryPageHeader
	ModuleColumnIndex
	ModuleOffsetIndex
	ModuleBloomFilterHeader
	ModuleBloomFilterBitset
)

// AAD constructs Additional Authenticated Data for an encrypted Parquet module.
func AAD(prefix, fileUnique []byte, moduleType ModuleType, rowGroupOrdinal, columnOrdinal, pageOrdinal int16) []byte {
	suffixLen := len(fileUnique) + 1
	if moduleType != ModuleFooter {
		suffixLen += 4
	}
	if moduleType == ModuleDataPage || moduleType == ModuleDataPageHeader {
		suffixLen += 2
	}

	aad := make([]byte, 0, len(prefix)+suffixLen)
	aad = append(aad, prefix...)
	aad = append(aad, fileUnique...)
	aad = append(aad, byte(moduleType))

	if moduleType != ModuleFooter {
		var buf [2]byte
		binary.LittleEndian.PutUint16(buf[:], uint16(rowGroupOrdinal))
		aad = append(aad, buf[:]...)
		binary.LittleEndian.PutUint16(buf[:], uint16(columnOrdinal))
		aad = append(aad, buf[:]...)
	}

	if moduleType == ModuleDataPage || moduleType == ModuleDataPageHeader {
		var buf [2]byte
		binary.LittleEndian.PutUint16(buf[:], uint16(pageOrdinal))
		aad = append(aad, buf[:]...)
	}

	return aad
}
