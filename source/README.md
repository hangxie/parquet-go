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

`ParquetFileReader.Open` returns a reader with an independent file handle for the requested file. The new reader may reuse the source's underlying storage client, but closing it must not invalidate the original reader.

Context-aware parquet operations use optional capability interfaces named `ContextReader`, `ContextWriter`, `ContextSeeker`, `ContextOpener`, `ContextCloner`, `ContextCreator`, and `ContextCloser`. Existing source implementations remain compatible without implementing them: the library checks cancellation before falling back to the original method, except that close always runs and reports cancellation afterward. Remote sources should implement the relevant optional interfaces so deadlines can cancel in-flight backend requests.

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
