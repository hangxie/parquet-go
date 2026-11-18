package swiftsource

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/ncw/swift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/writer"
)

func TestSwiftFileInterfaceCompliance(t *testing.T) {
	var _ source.ParquetFileReader = (*swiftReader)(nil)
	var _ source.ParquetFileWriter = (*swiftWriter)(nil)
}

func TestSwiftReaderCloseWithNilFileReader(t *testing.T) {
	reader := &swiftReader{
		swiftFile: swiftFile{
			connection: &swift.Connection{},
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileReader: nil,
	}

	err := reader.Close()
	require.NoError(t, err)
}

// Test that these functions exist and can be called (even if they fail)
func TestNewSwiftFileReaderExists(t *testing.T) {
	// Just test that the function exists and can be called
	fn := reflect.ValueOf(NewSwiftFileReader)
	require.True(t, fn.IsValid())

	// Verify function signature
	fnType := fn.Type()
	require.Equal(t, 3, fnType.NumIn())
	require.Equal(t, 2, fnType.NumOut())
}

func TestSwiftReaderMethodsExist(t *testing.T) {
	reader := &swiftReader{}

	// Verify all required methods exist
	readerType := reflect.TypeOf(reader)

	methods := []string{"Read", "Seek", "Close", "Open", "Clone"}
	for _, methodName := range methods {
		_, exists := readerType.MethodByName(methodName)
		require.True(t, exists)
	}
}

// Test method signatures
func TestSwiftReaderMethodSignatures(t *testing.T) {
	reader := &swiftReader{}
	readerType := reflect.TypeOf(reader)

	// Test Read method signature
	readMethod, exists := readerType.MethodByName("Read")
	require.True(t, exists)
	readType := readMethod.Type
	require.Equal(t, 2, readType.NumIn())
	require.Equal(t, 2, readType.NumOut())

	// Test Seek method signature
	seekMethod, exists := readerType.MethodByName("Seek")
	require.True(t, exists)
	seekType := seekMethod.Type
	require.Equal(t, 3, seekType.NumIn())
	require.Equal(t, 2, seekType.NumOut())

	// Test Close method signature
	closeMethod, exists := readerType.MethodByName("Close")
	require.True(t, exists)
	closeType := closeMethod.Type
	require.Equal(t, 1, closeType.NumIn())
	require.Equal(t, 1, closeType.NumOut())
}

// Test struct field access
func TestSwiftReaderFieldAccess(t *testing.T) {
	reader := &swiftReader{
		swiftFile: swiftFile{
			connection: &swift.Connection{},
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileReader: nil,
	}

	// Test that we can access embedded fields
	require.Equal(t, "test-container", reader.container)
	require.Equal(t, "test.parquet", reader.filePath)
	require.NotNil(t, reader.connection)
}

// Test that method calls are delegated to the correct fields
func TestSwiftReaderReadDelegation(t *testing.T) {
	reader := &swiftReader{
		swiftFile: swiftFile{
			connection: &swift.Connection{},
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileReader: nil,
	}

	buf := make([]byte, 10)
	_, err := reader.Read(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fileReader is nil")
}

func TestSwiftReaderSeekDelegation(t *testing.T) {
	reader := &swiftReader{
		swiftFile: swiftFile{
			connection: &swift.Connection{},
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileReader: nil,
	}

	_, err := reader.Seek(0, io.SeekStart)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fileReader is nil")
}

func TestSwiftReaderStructure(t *testing.T) {
	conn := &swift.Connection{}

	reader := &swiftReader{
		swiftFile: swiftFile{
			connection: conn,
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileReader: nil,
	}

	require.Equal(t, conn, reader.connection)
	require.Equal(t, "test-container", reader.container)
	require.Equal(t, "test.parquet", reader.filePath)
}

func TestSwiftCloneIsLazy(t *testing.T) {
	conn := setupSwiftServer(t)
	require.NoError(t, conn.ObjectPutBytes(testContainer, "lazy.parquet", []byte("data"), ""))
	original, err := NewSwiftFileReader(testContainer, "lazy.parquet", conn)
	require.NoError(t, err)
	defer func() { require.NoError(t, original.Close()) }()
	conn.StorageUrl = "http://127.0.0.1:1"
	clone, err := original.Clone()
	require.NoError(t, err, "cloning must not issue an HTTP request")
	require.NoError(t, clone.Close())
}

func TestSwiftWideParquetReader(t *testing.T) {
	ctx := context.Background()
	var data bytes.Buffer
	metadata := make([]string, 160)
	row := make([]any, len(metadata))
	for i := range metadata {
		metadata[i] = fmt.Sprintf("name=c%d, type=INT64", i)
		row[i] = int64(i)
	}
	pw, err := writer.NewCSVWriterFromWriterWithContext(ctx, metadata, &data)
	require.NoError(t, err)
	require.NoError(t, pw.WriteWithContext(ctx, row))
	require.NoError(t, pw.WriteStopWithContext(ctx))
	tracker := &requestTracker{}
	conn := setupSwiftServer(t, tracker)
	require.NoError(t, conn.ObjectPutBytes(testContainer, "wide.parquet", data.Bytes(), ""))
	file, err := NewSwiftFileReader(testContainer, "wide.parquet", conn)
	require.NoError(t, err)
	pr, err := reader.NewParquetReaderWithContext(ctx, file, nil, reader.WithNP(4))
	require.NoError(t, err)
	initialGets, _, _ := tracker.counts()
	require.Less(t, initialGets, len(metadata), "initializing columns must not open a stream per column")
	for range 2 {
		rows, err := pr.ReadByNumberWithContext(ctx, 1)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		value := reflect.ValueOf(rows[0])
		for i := range metadata {
			require.EqualValues(t, i, value.Field(i).Int())
		}
		require.NoError(t, pr.ResetWithContext(ctx))
	}
	require.NoError(t, pr.ReadStopWithContext(ctx))
	require.NoError(t, file.Close())
	_, active, peak := tracker.counts()
	require.Zero(t, active)
	require.LessOrEqual(t, peak, defaultStreamLimit)
}
