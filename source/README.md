# parquet-go-source

parquet-go-source is a source provider for parquet-go. Sources implement separate reader and writer interfaces:

```go
type ParquetFileReader interface {
	io.Seeker
	io.Reader
	io.Closer
	Open(name string) (ParquetFileReader, error)
	Clone() (ParquetFileReader, error)
}

type ParquetFileWriter interface {
	io.Writer
	io.Closer
	Create(name string) (ParquetFileWriter, error)
}
```

Supported sources:
* Local
* HDFS
* S3 (AWS SDK v1 and v2)
* Google Cloud Storage
* Azure Blob Storage
* HTTP (read-only)
* Memory buffer
* GoCloud CDK (generic blob storage)
* OpenStack Swift

Thanks for all the contributors!
