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

## Swift

Swift readers created by `NewSwiftFileReader` share a pool capped at 256 open object streams across their clones and readers returned by `Open`. Each active stream has its own position, so reads on different cursors can run concurrently without seeking a shared HTTP body. `NewSwiftFileReaderWithStreamLimit(container, path, conn, limit)` accepts any positive limit for workloads needing a different connection budget. Separate calls to either constructor create independent pools.

For scenario-based sizing and benchmarking advice, see [Swift stream limits](../README.md#swift-stream-limits).

Constructors and `Open` use HEAD to fetch the object size and ETag without opening a stream. GET permission and stream-open errors surface on the first read. Every GET uses `If-Match` and validates the returned ETag against that metadata; clones retain the same ETag, so an object overwritten between reads fails rather than mixing versions. Resumed GETs must also provide a valid `Content-Range` matching the saved offset, expected end, and object size; invalid responses are closed before any bytes are returned. Call `Open` to read a new version.

Readers require an ETag and reject metadata without one. Large-object manifests must provide consistent assembled-object ETags across HEAD and ranged GET responses. Live DLO/SLO interoperability on Swift and Ceph is not exercised by the current integration tests.

Clones are lazy: `Clone` does not issue an HTTP request, and errors opening a clone's stream surface on its first read. `Clone` on a closed Swift reader returns `os.ErrClosed`; use `Open` to open a new reader by path. Closing one reader does not invalidate its clones or other readers in the pool. Call `ReadStop` and close the original source reader to release their streams.

The pool keeps recently used streams for sequential reads. Once the limit is reached, it closes an idle stream and resumes that cursor later using an HTTP range request at its saved offset. If all streams are reading, further reads wait for a slot. Interleaving more than 256 retained cursors with the default limit (or more than a custom limit) can cause repeated range requests and lower throughput; increase the limit when retaining more streams fits the application's connection budget. Pool waits cannot currently be interrupted by context cancellation. Seeks update the cursor position without requests, and reading at or beyond EOF returns EOF without opening a stream.

## Reader lifetimes

Local-file clones share one reference-counted `*os.File` and keep independent positions. Reads use `ReadAt`, allowing parallel column access without opening another file descriptor. A local clone continues to read the same open file even if its path is renamed or removed. The handle closes after the last reader or clone closes. `Clone` on a closed local reader now returns `os.ErrClosed`, even if another clone remains open; previously it reopened the file by path. Clone before closing the source, or use `Open` to obtain a new reader by path. Call `ReadStop` to release the column readers, and close the original source reader when finished. `Open` creates a separate handle.

HDFS readers fetch the file size with a namenode metadata request. Clones start at offset zero without an RPC. Each cursor opens its own data stream on its first nonempty read before EOF and keeps it for sequential reads. Independent cursors can read concurrently. The default limit is 256 open streams across the original reader, its clones, and readers created by `Open`. Use `NewHdfsFileReaderWithStreamLimit` to set a different positive limit.

At capacity, the pool closes the least recently used idle stream. Its cursor reopens the path and seeks to the saved offset on its next read. When all streams are busy, reads wait for a slot. Pool waits cannot currently be interrupted by context cancellation. Interleaving more active columns than the limit can cause repeated namenode opens and datanode reconnects. Keep input paths stable throughout the read, including after eviction. Stream-open errors can surface on the first read or after eviction.

Each reader owns a reference to the HDFS client. Close each explicitly opened reader and call `ReadStop` to release column cursors. Closing the original reader leaves derived readers usable, and the final close releases the client. Cloning or opening a path from a closed reader returns `os.ErrClosed`.

Thanks for all the contributors!
