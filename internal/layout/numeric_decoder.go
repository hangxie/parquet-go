package layout

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func numericWidth(typ parquet.Type) uint64 {
	switch typ {
	case parquet.Type_INT32, parquet.Type_FLOAT:
		return 4
	case parquet.Type_INT64, parquet.Type_DOUBLE:
		return 8
	default:
		return 0
	}
}

func numericValue(typ parquet.Type, bits uint64) any {
	switch typ {
	case parquet.Type_INT32:
		return int32(bits)
	case parquet.Type_FLOAT:
		return math.Float32frombits(uint32(bits))
	case parquet.Type_DOUBLE:
		return math.Float64frombits(bits)
	default:
		return int64(bits)
	}
}

func (d *valueDecoder) nextPlain() (any, error) {
	if width := numericWidth(d.dataType); width > 0 {
		if uint64(d.reader.Len()) < width {
			return nil, io.ErrUnexpectedEOF
		}
		var data [8]byte
		// The fixed-width value is present, so a bytes.Reader read cannot be short.
		_, _ = d.reader.Read(data[:width])
		return numericValue(d.dataType, binary.LittleEndian.Uint64(data[:])), nil
	}
	values, err := encoding.ReadPlain(d.reader, d.dataType, 1, d.width)
	if err != nil {
		return nil, err
	}
	return values[0], nil
}

func (d *valueDecoder) nextSplit() any {
	if d.dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		data := make([]byte, d.width)
		for i := range d.width {
			data[i] = d.split[i*d.count+d.index-1]
		}
		return string(data)
	}
	var bits uint64
	for i := range d.width {
		bits |= uint64(d.split[i*d.count+d.index-1]) << (8 * i)
	}
	return numericValue(d.dataType, bits)
}
