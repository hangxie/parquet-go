package compress

import "github.com/hangxie/parquet-go/v2/parquet"

func init() {
	compressors[parquet.CompressionCodec_UNCOMPRESSED] = &Compressor{
		Compress: func(buf []byte) []byte {
			return buf
		},
		Uncompress: func(buf []byte) (bytes []byte, err error) {
			return buf, nil
		},
	}
}
