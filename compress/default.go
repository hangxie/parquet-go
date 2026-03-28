package compress

import "github.com/hangxie/parquet-go/v3/parquet"

func init() {
	compressors[parquet.CompressionCodec_UNCOMPRESSED] = &Compressor{
		Compress: func(buf []byte) ([]byte, error) {
			return buf, nil
		},
		Uncompress: func(buf []byte) (bytes []byte, err error) {
			return buf, nil
		},
	}
}
