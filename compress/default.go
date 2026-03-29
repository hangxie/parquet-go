package compress

import "github.com/hangxie/parquet-go/v3/parquet"

func init() {
	defaultCodecs[parquet.CompressionCodec_UNCOMPRESSED] = &codec{
		compress: func(buf []byte) ([]byte, error) {
			return buf, nil
		},
		uncompress: func(buf []byte, _ int64) ([]byte, error) {
			return buf, nil
		},
	}
}
