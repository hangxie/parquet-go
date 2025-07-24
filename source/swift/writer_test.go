package swiftsource

import (
	"reflect"
	"testing"

	"github.com/ncw/swift"
	"github.com/stretchr/testify/require"
)

func Test_SwiftWriterCloseWithNilFileWriter(t *testing.T) {
	writer := &swiftWriter{
		swiftFile: swiftFile{
			connection: &swift.Connection{},
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileWriter: nil,
	}

	err := writer.Close()
	require.NoError(t, err)
}

func Test_SwiftWriterMethodsExist(t *testing.T) {
	writer := &swiftWriter{}

	// Verify all required methods exist
	writerType := reflect.TypeOf(writer)

	methods := []string{"Write", "Close", "Create"}
	for _, methodName := range methods {
		_, exists := writerType.MethodByName(methodName)
		require.True(t, exists, "Method %s does not exist on swiftWriter", methodName)
	}
}

func Test_SwiftWriterMethodSignatures(t *testing.T) {
	writer := &swiftWriter{}
	writerType := reflect.TypeOf(writer)

	// Test Write method signature
	writeMethod, exists := writerType.MethodByName("Write")
	require.True(t, exists)
	writeType := writeMethod.Type
	require.Equal(t, 2, writeType.NumIn())
	require.Equal(t, 2, writeType.NumOut())
}

func Test_SwiftWriterFieldAccess(t *testing.T) {
	writer := &swiftWriter{
		swiftFile: swiftFile{
			connection: &swift.Connection{},
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileWriter: nil,
	}

	// Test that we can access embedded fields
	require.Equal(t, "test-container", writer.container)
	require.Equal(t, "test.parquet", writer.filePath)
	require.NotNil(t, writer.connection)
}

func Test_SwiftWriterWriteDelegation(t *testing.T) {
	writer := &swiftWriter{
		swiftFile: swiftFile{
			connection: &swift.Connection{},
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileWriter: nil,
	}

	testData := []byte("test")
	require.Panics(t, func() {
		_, _ = writer.Write(testData)
	})
}

func Test_SwiftFileStructure(t *testing.T) {
	conn := &swift.Connection{}

	file := swiftFile{
		connection: conn,
		container:  "test-container",
		filePath:   "test.parquet",
	}

	require.Equal(t, conn, file.connection)
	require.Equal(t, "test-container", file.container)
	require.Equal(t, "test.parquet", file.filePath)
}

func Test_SwiftWriterStructure(t *testing.T) {
	conn := &swift.Connection{}

	writer := &swiftWriter{
		swiftFile: swiftFile{
			connection: conn,
			container:  "test-container",
			filePath:   "test.parquet",
		},
		fileWriter: nil,
	}

	require.Equal(t, conn, writer.connection)
	require.Equal(t, "test-container", writer.container)
	require.Equal(t, "test.parquet", writer.filePath)
}
