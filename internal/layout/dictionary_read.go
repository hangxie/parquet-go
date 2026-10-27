package layout

import (
	"bytes"
	"fmt"

	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func readDictionaryValues(reader *bytes.Reader, dataType parquet.Type, count int32, width uint64) ([]any, error) {
	if count < 0 {
		return nil, fmt.Errorf("negative dictionary value count %d", count)
	}
	var minimum uint64
	switch dataType {
	case parquet.Type_BOOLEAN:
		minimum = (uint64(count) + 7) / 8
	case parquet.Type_INT32, parquet.Type_FLOAT, parquet.Type_BYTE_ARRAY:
		minimum = uint64(count) * 4
	case parquet.Type_INT64, parquet.Type_DOUBLE:
		minimum = uint64(count) * 8
	case parquet.Type_INT96:
		minimum = uint64(count) * 12
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if width == 0 && count > 0 {
			return nil, fmt.Errorf("dictionary fixed length must be positive")
		}
		// Compare before multiplying so invalid schema lengths cannot wrap.
		if count > 0 && width > uint64(reader.Len())/uint64(count) {
			return nil, fmt.Errorf("dictionary values exceed remaining data %d", reader.Len())
		}
		minimum = uint64(count) * width
	}
	// Dictionary entries use PLAIN encoding: unlike data runs, each requires input bytes.
	if minimum > uint64(reader.Len()) {
		return nil, fmt.Errorf("dictionary values need at least %d bytes but %d remain", minimum, reader.Len())
	}
	return encoding.ReadPlain(reader, dataType, uint64(count), width)
}
