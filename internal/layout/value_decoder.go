package layout

import (
	"bytes"
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

type valueDecoder struct {
	reader   *bytes.Reader
	method   parquet.Encoding
	dataType parquet.Type
	width    uint64
	count    uint64
	index    uint64
	ints     *encoding.IntDecoder
	delta    *encoding.DeltaDecoder
	prefixes *encoding.DeltaDecoder
	previous string
	split    []byte
}

func newValueDecoder(reader *bytes.Reader, method parquet.Encoding, dataType parquet.Type, count, width uint64) (*valueDecoder, error) {
	d := &valueDecoder{reader: reader, method: method, dataType: dataType, count: count, width: width}
	if count == 0 {
		return d, nil
	}
	var err error
	switch method {
	case parquet.Encoding_RLE_DICTIONARY, parquet.Encoding_PLAIN_DICTIONARY:
		var b byte
		b, err = reader.ReadByte()
		if err == nil {
			d.ints, err = encoding.NewIntDecoder(reader, uint64(b), count, true, false)
		}
	case parquet.Encoding_RLE, parquet.Encoding_BIT_PACKED:
		d.ints, err = encoding.NewIntDecoder(reader, defaultBitWidth(dataType), count, method == parquet.Encoding_RLE, method == parquet.Encoding_RLE)
	case parquet.Encoding_PLAIN:
		if dataType == parquet.Type_BOOLEAN {
			d.ints, err = encoding.NewIntDecoder(reader, 1, count, false, false)
		}
	case parquet.Encoding_DELTA_BINARY_PACKED:
		if dataType != parquet.Type_INT32 && dataType != parquet.Type_INT64 {
			return nil, fmt.Errorf("DELTA_BINARY_PACKED requires INT32 or INT64")
		}
		d.delta, err = encoding.NewDeltaDecoder(reader, count, dataType == parquet.Type_INT32)
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		d.prefixes, err = openDeltaSection(reader, count)
		if err == nil {
			d.delta, err = openDeltaSection(reader, count)
		}
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		d.delta, err = openDeltaSection(reader, count)
	case parquet.Encoding_BYTE_STREAM_SPLIT:
		switch dataType {
		case parquet.Type_FLOAT, parquet.Type_INT32:
			d.width = 4
		case parquet.Type_DOUBLE, parquet.Type_INT64:
			d.width = 8
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		default:
			return nil, fmt.Errorf("unsupported BYTE_STREAM_SPLIT type %v", dataType)
		}
		if d.width == 0 || count > uint64(reader.Len())/d.width {
			return nil, io.ErrUnexpectedEOF
		}
		d.split = make([]byte, count*d.width)
		_, err = io.ReadFull(reader, d.split)
	default:
		return nil, fmt.Errorf("unknown Encoding method: %v", method)
	}
	if err != nil {
		return nil, err
	}
	return d, nil
}

func openDeltaSection(reader *bytes.Reader, count uint64) (*encoding.DeltaDecoder, error) {
	copyReader := *reader
	d, err := encoding.NewDeltaDecoder(&copyReader, count, false)
	if err != nil {
		return nil, err
	}
	end, err := d.End()
	if err != nil {
		return nil, err
	}
	_, _ = reader.Seek(end, io.SeekStart)
	return d, nil
}

func (d *valueDecoder) next() (any, error) {
	if d.index >= d.count {
		return nil, io.EOF
	}
	d.index++
	if d.ints != nil {
		v, err := d.ints.Next()
		if err != nil {
			return nil, err
		}
		if d.method == parquet.Encoding_RLE_DICTIONARY || d.method == parquet.Encoding_PLAIN_DICTIONARY {
			return v, nil
		}
		switch d.dataType {
		case parquet.Type_BOOLEAN:
			return v != 0, nil
		case parquet.Type_INT32:
			return int32(v), nil
		default:
			return v, nil
		}
	}
	switch d.method {
	case parquet.Encoding_DELTA_BINARY_PACKED:
		v, err := d.delta.Next()
		if d.dataType == parquet.Type_INT32 {
			return int32(v), err
		}
		return v, err
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
		return d.nextString()
	case parquet.Encoding_BYTE_STREAM_SPLIT:
		data := make([]byte, d.width)
		for i := range d.width {
			data[i] = d.split[i*d.count+d.index-1]
		}
		values, err := encoding.ReadPlain(bytes.NewReader(data), d.dataType, 1, d.width)
		if err != nil {
			return nil, err
		}
		return values[0], nil
	default:
		values, err := encoding.ReadPlain(d.reader, d.dataType, 1, d.width)
		if err != nil {
			return nil, err
		}
		return values[0], nil
	}
}

func (d *valueDecoder) nextString() (any, error) {
	prefix := int64(0)
	var err error
	if d.prefixes != nil {
		prefix, err = d.prefixes.Next()
		if err != nil {
			return nil, err
		}
		if prefix < 0 || prefix > int64(len(d.previous)) {
			return nil, fmt.Errorf("invalid prefix length %d", prefix)
		}
	}
	length, err := d.delta.Next()
	if err != nil {
		return nil, err
	}
	if length < 0 || length > int64(d.reader.Len()) {
		return nil, fmt.Errorf("invalid byte array length %d with %d bytes remaining", length, d.reader.Len())
	}
	suffix := make([]byte, length)
	// The byte length was checked against the remaining input above.
	_, _ = d.reader.Read(suffix)
	d.previous = d.previous[:prefix] + string(suffix)
	return d.previous, nil
}

func (d *valueDecoder) skip(count uint64) error {
	if count > d.count-d.index {
		return io.ErrUnexpectedEOF
	}
	if d.ints != nil {
		if err := d.ints.Skip(count); err != nil {
			return err
		}
		d.index += count
		return nil
	}
	if d.method == parquet.Encoding_DELTA_BINARY_PACKED {
		if err := d.delta.Skip(count); err != nil {
			return err
		}
		d.index += count
		return nil
	}
	for range count {
		if _, err := d.next(); err != nil {
			return err
		}
	}
	return nil
}

// defaultBitWidth returns the default bitWidth for a data type when not specified.
func defaultBitWidth(dataType parquet.Type) uint64 {
	switch dataType {
	case parquet.Type_BOOLEAN:
		return 1
	case parquet.Type_INT32:
		return 32
	case parquet.Type_INT64:
		return 64
	default:
		return 0
	}
}
