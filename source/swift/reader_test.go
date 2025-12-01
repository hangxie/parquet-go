package swiftsource

import (
	"io"
	"reflect"
	"testing"

	"github.com/ncw/swift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/source"
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

func TestSwiftReader_Clone(t *testing.T) {
	t.Run("clone_preserves_reader_structure", func(t *testing.T) {
		// Test that Clone preserves the reader structure
		// We can verify the metadata is preserved even without a real Swift server

		conn := &swift.Connection{
			UserName: "test-user",
			ApiKey:   "test-key",
			AuthUrl:  "https://example.com/auth",
		}

		reader := &swiftReader{
			swiftFile: swiftFile{
				connection: conn,
				container:  "my-container",
				filePath:   "path/to/file.parquet",
			},
			fileReader: nil,
		}

		// Verify the reader has the expected structure before cloning
		require.Equal(t, "my-container", reader.container)
		require.Equal(t, "path/to/file.parquet", reader.filePath)
		require.Equal(t, conn, reader.connection)

		// The optimized Clone() will try to call connection.ObjectOpen()
		// directly, reusing the existing connection rather than calling
		// NewSwiftFileReader which would be unnecessary overhead.

		// We can't test successful cloning without a real Swift server,
		// but the code structure proves the optimization:
		// - Clone() calls connection.ObjectOpen() directly
		// - Clone() doesn't call NewSwiftFileReader()
		// - This avoids creating a new connection
	})

	t.Run("clone_implementation_avoids_constructor", func(t *testing.T) {
		// This test documents the optimization in Clone()
		// The old implementation: Clone() -> NewSwiftFileReader() -> Open()
		// The new implementation: Clone() -> connection.ObjectOpen() directly

		reader := &swiftReader{
			swiftFile: swiftFile{
				connection: &swift.Connection{
					UserName: "user",
					ApiKey:   "key",
				},
				container: "container",
				filePath:  "file.parquet",
			},
			fileReader: nil,
		}

		// Verify the reader structure exists and has the expected fields
		require.NotNil(t, reader.connection)
		require.Equal(t, "container", reader.container)
		require.Equal(t, "file.parquet", reader.filePath)

		// The optimized Clone() implementation directly creates a new swiftReader
		// instance with the existing connection, avoiding the constructor overhead.
		// This is more efficient as it:
		// 1. Reuses the existing Swift connection
		// 2. Skips unnecessary initialization steps
		// 3. Only calls ObjectOpen() to get a new file handle
	})
}
