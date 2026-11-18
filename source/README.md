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
* S3 (AWS SDK v2)
* Google Cloud Storage
* Azure Blob Storage
* HTTP (read-only)
* Memory buffer
* GoCloud CDK (generic blob storage)
* OpenStack Swift

## Google Cloud Storage

`gcs.NewGcsFileReader` and `gcs.NewGcsFileWriter` create their own storage client with `storage.NewClient(ctx)` and close it in `Close`. That client authenticates with [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials). To authenticate differently, for example anonymously for a public bucket, build the client yourself and pass it to `gcs.NewGcsFileReaderWithClient` or `gcs.NewGcsFileWriterWithClient`; a client supplied this way is left open for the caller to close.

The `projectID` argument is only recorded on the source; object access is authorized against the bucket and does not depend on it. A reader `generation` of `-1` reads the latest generation of the object.

A written object appears only when the writer's `Close` commits the upload, so upload errors such as a missing bucket surface at `Close`, and canceling the writer's context before `Close` leaves no object behind. See [example/gcs](../example/gcs) for a round trip.

## Reader lifetimes

Local-file clones share one reference-counted `*os.File` and keep independent positions. Reads use `ReadAt`, allowing parallel column access without opening another file descriptor. A local clone continues to read the same open file even if its path is renamed or removed. The handle closes after the last reader or clone closes. `Clone` on a closed local reader now returns `os.ErrClosed`, even if another clone remains open; previously it reopened the file by path. Clone before closing the source, or use `Open` to obtain a new reader by path. Call `ReadStop` to release the column readers, and close the original source reader when finished. `Open` creates a separate handle.

HDFS and Swift clones continue to open independent streams. Sharing those streams would serialize reads and could trigger costly reconnects or HTTP range requests when columns interleave; wide-file handle limits for these sources remain unresolved. HDFS `ReadAt` also changes the stream position internally and cannot be used for concurrent column reads without synchronization.

Thanks for all the contributors!
