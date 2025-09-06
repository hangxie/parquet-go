//go:build example

package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"math"

	"github.com/hangxie/parquet-go/v2/marshal"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

// Row showcases newly added logical types
type Row struct {
	// FLOAT16 stored as FIXED[2]
	F16 string `parquet:"name=F16, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16"`

	// INTEGER logical type examples
	I8  int32 `parquet:"name=I8, type=INT32, logicaltype=INTEGER, logicaltype.bitwidth=8, logicaltype.issigned=true"`
	U16 int32 `parquet:"name=U16, type=INT32, logicaltype=INTEGER, logicaltype.bitwidth=16, logicaltype.issigned=false"`

	// UUID (not new, but a useful reference)
	UUID string `parquet:"name=UUID, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID"`
}

func main() {
	// write
	fw, err := local.NewLocalFileWriter("new-logical.parquet")
	if err != nil {
		log.Fatal(err)
	}
	pw, err := writer.NewParquetWriter(fw, new(Row), 1)
	if err != nil {
		log.Fatal(err)
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// 3 sample rows
	rows := []Row{
		{F16: f32ToF16Bytes(0.5), I8: -10, U16: 1000, UUID: hexToFixed16("0123456789abcdef0123456789abcdef")},
		{F16: f32ToF16Bytes(1.0), I8: 0, U16: 65535 - 100, UUID: hexToFixed16("89abcdef012345670123456789abcdef")},
		{F16: f32ToF16Bytes(-2.0), I8: 120, U16: 42, UUID: hexToFixed16("00112233445566778899aabbccddeeff")},
	}
	for _, r := range rows {
		if err := pw.Write(r); err != nil {
			log.Fatal(err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		log.Fatal(err)
	}
	_ = fw.Close()

	// read + convert to JSON-friendly (applies logical-type conversions)
	fr, err := local.NewLocalFileReader("new-logical.parquet")
	if err != nil {
		log.Fatal(err)
	}
	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Fatal(err)
	}
	n := int(pr.GetNumRows())
	data, err := pr.ReadByNumber(n)
	if err != nil {
		log.Fatal(err)
	}
	converted, err := marshal.ConvertToJSONFriendly(data, pr.SchemaHandler)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\n", converted)
	pr.ReadStop()
	_ = fr.Close()
}

// f32ToF16Bytes converts a float32 into IEEE 754 binary16 (big-endian 2 bytes) as string
func f32ToF16Bytes(f float32) string {
	// Handle NaN/Inf explicitly
	if f != f { // NaN
		return string([]byte{0x7e, 0x00})
	}
	if f > 65504 { // max half
		return string([]byte{0x7c, 0x00})
	}
	if f < -65504 {
		return string([]byte{0xfc, 0x00})
	}
	bits := math.Float32bits(f)
	sign := uint16((bits >> 31) & 0x1)
	exp := int32((bits>>23)&0xff) - 127 + 15 // re-bias
	mant := uint32(bits & 0x7fffff)

	var half uint16
	if exp <= 0 {
		// subnormal or zero in half
		if exp < -10 {
			half = sign << 15 // zero
		} else {
			// add implicit leading 1 to mantissa
			mant = (mant | 0x800000) >> uint32(1-exp)
			// round to nearest
			mant = (mant + 0x1000) >> 13
			half = (sign << 15) | uint16(mant)
		}
	} else if exp >= 31 {
		// overflow -> inf
		half = (sign << 15) | (0x1f << 10)
	} else {
		// normalized
		// round mantissa
		mant = mant + 0x1000
		if mant&0x800000 != 0 {
			// mant overflow -> adjust exp
			mant = 0
			exp++
			if exp >= 31 {
				half = (sign << 15) | (0x1f << 10)
				return string([]byte{byte(half >> 8), byte(half)})
			}
		}
		half = (sign << 15) | (uint16(exp) << 10) | uint16(mant>>13)
	}
	return string([]byte{byte(half >> 8), byte(half)})
}

// hexToFixed16 converts a 32-hex-character string into 16 raw bytes as string
func hexToFixed16(h string) string {
	bs, err := hex.DecodeString(h)
	if err != nil || len(bs) != 16 {
		return string(make([]byte, 16))
	}
	return string(bs)
}
