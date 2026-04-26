package layout

import (
	"bytes"
	"fmt"

	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// convertRLEValuesForType converts int64 RLE values to the target data type (int32 or bool).
func convertRLEValuesForType(values []any, dataType parquet.Type) {
	switch dataType {
	case parquet.Type_INT32:
		for i := range values {
			values[i] = int32(values[i].(int64))
		}
	case parquet.Type_BOOLEAN:
		for i := range values {
			values[i] = values[i].(int64) > 0
		}
	}
}

// readByteStreamSplit reads values using the BYTE_STREAM_SPLIT encoding for the given data type.
func readByteStreamSplit(bytesReader *bytes.Reader, dataType parquet.Type, cnt, bitWidth uint64) ([]any, error) {
	switch dataType {
	case parquet.Type_FLOAT:
		return encoding.ReadByteStreamSplitFloat32(bytesReader, cnt)
	case parquet.Type_DOUBLE:
		return encoding.ReadByteStreamSplitFloat64(bytesReader, cnt)
	case parquet.Type_INT32:
		return encoding.ReadByteStreamSplitINT32(bytesReader, cnt)
	case parquet.Type_INT64:
		return encoding.ReadByteStreamSplitINT64(bytesReader, cnt)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return encoding.ReadByteStreamSplitFixedLenByteArray(bytesReader, cnt, bitWidth)
	default:
		return nil, fmt.Errorf("the encoding method BYTE_STREAM_SPLIT is only supported for FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY, got %v", dataType)
	}
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

func readDeltaBinaryPacked(bytesReader *bytes.Reader, dataType parquet.Type) ([]any, error) {
	switch dataType {
	case parquet.Type_INT32:
		return encoding.ReadDeltaBinaryPackedINT32(bytesReader)
	case parquet.Type_INT64:
		return encoding.ReadDeltaBinaryPackedINT64(bytesReader)
	default:
		return nil, fmt.Errorf("the encoding method DELTA_BINARY_PACKED can only be used with int32 and int64 types, got %v", dataType)
	}
}

func readDeltaByteArrayValues(values []any, err error, dataType parquet.Type, cnt uint64) ([]any, error) {
	if err != nil {
		return nil, err
	}
	if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		for i := range values {
			values[i] = values[i].(string)
		}
	}
	return values[:cnt], nil
}

// Read data page values
func ReadDataPageValues(bytesReader *bytes.Reader, encodingMethod parquet.Encoding, dataType parquet.Type, convertedType parquet.ConvertedType, cnt, bitWidth uint64) ([]any, error) {
	var res []any
	if cnt <= 0 {
		return res, nil
	}

	if bitWidth == 0 {
		bitWidth = defaultBitWidth(dataType)
	}

	switch encodingMethod {
	case parquet.Encoding_PLAIN:
		return encoding.ReadPlain(bytesReader, dataType, cnt, bitWidth)
	case parquet.Encoding_PLAIN_DICTIONARY, parquet.Encoding_RLE_DICTIONARY:
		b, err := bytesReader.ReadByte()
		if err != nil {
			return res, err
		}
		bitWidth = uint64(b)

		buf, err := encoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, uint64(bytesReader.Len()))
		if err != nil {
			return res, err
		}
		if uint64(len(buf)) < cnt {
			return res, fmt.Errorf("expected %d values but got %d from RLE/bit-packed hybrid decoder", cnt, len(buf))
		}
		return buf[:cnt], err
	case parquet.Encoding_RLE:
		values, err := encoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, 0)
		if err != nil {
			return res, err
		}
		if uint64(len(values)) < cnt {
			return res, fmt.Errorf("expected %d values but got %d from RLE/bit-packed hybrid decoder", cnt, len(values))
		}
		convertRLEValuesForType(values, dataType)
		return values[:cnt], nil
	case parquet.Encoding_BIT_PACKED:
		values, err := encoding.ReadBitPackedCount(bytesReader, cnt, bitWidth)
		if err != nil {
			return res, err
		}
		convertRLEValuesForType(values, dataType)
		return values, nil
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return readDeltaBinaryPacked(bytesReader, dataType)
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		values, err := encoding.ReadDeltaLengthByteArray(bytesReader)
		return readDeltaByteArrayValues(values, err, dataType, cnt)
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		values, err := encoding.ReadDeltaByteArray(bytesReader)
		return readDeltaByteArrayValues(values, err, dataType, cnt)
	case parquet.Encoding_BYTE_STREAM_SPLIT:
		return readByteStreamSplit(bytesReader, dataType, cnt, bitWidth)
	default:
		return res, fmt.Errorf("unknown Encoding method: %v", encodingMethod)
	}
}
