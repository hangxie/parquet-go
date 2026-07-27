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

## Reader `Open` and the `InPlaceReopener` capability

`Open` is used to open a sibling file referenced by a column chunk's
`file_path`. Most backends return a brand-new, independent reader from `Open`;
the caller owns that reader and closes the previous handle itself.

A backend whose `Open` instead reopens its own internal handle and returns the
same receiver (the HDFS source works this way, reusing a single client) must
declare the optional `InPlaceReopener` capability so callers do not close the
handle they just reopened:

```go
type InPlaceReopener interface {
	ReopensInPlace() bool
}
```

When a reader reports `ReopensInPlace() == true`, callers that swap in the
result of `Open` skip closing the previous handle, because the previous and new
readers are the same object. Backends whose `Open` yields an independent reader
should not implement this interface (or should return `false`). Use the
`source.ReopensInPlace(reader)` helper to query the capability safely.

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
