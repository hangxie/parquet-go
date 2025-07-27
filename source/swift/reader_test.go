package swiftsource

import (
	"io"
	"reflect"
	"testing"

	"github.com/ncw/swift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/source"
)

func Test_SwiftFileInterfaceCompliance(t *testing.T) {
	var _ source.ParquetFileReader = (*swiftReader)(nil)
	var _ source.ParquetFileWriter = (*swiftWriter)(nil)
}

func Test_SwiftReaderCloseWithNilFileReader(t *testing.T) {
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
func Test_NewSwiftFileReaderExists(t *testing.T) {
	// Just test that the function exists and can be called
	fn := reflect.ValueOf(NewSwiftFileReader)
	require.True(t, fn.IsValid())

	// Verify function signature
	fnType := fn.Type()
	require.Equal(t, 3, fnType.NumIn())
	require.Equal(t, 2, fnType.NumOut())
}

func Test_SwiftReaderMethodsExist(t *testing.T) {
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
func Test_SwiftReaderMethodSignatures(t *testing.T) {
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
func Test_SwiftReaderFieldAccess(t *testing.T) {
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
func Test_SwiftReaderReadDelegation(t *testing.T) {
	reader := &swiftReader{
		swiftFile: swiftFile{
			connection: &swift.Connection{},
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileReader: nil,
	}

	buf := make([]byte, 10)
	require.Panics(t, func() {
		_, _ = reader.Read(buf)
	})
}

func Test_SwiftReaderSeekDelegation(t *testing.T) {
	reader := &swiftReader{
		swiftFile: swiftFile{
			connection: &swift.Connection{},
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileReader: nil,
	}

	require.Panics(t, func() {
		_, _ = reader.Seek(0, io.SeekStart)
	})
}

func Test_SwiftReaderStructure(t *testing.T) {
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
