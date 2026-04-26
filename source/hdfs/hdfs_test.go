package hdfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Compile-time check: realHdfsClient satisfies hdfsClientIface.
var _ hdfsClientIface = (*realHdfsClient)(nil)

func TestRealHdfsClient_InterfaceFields(t *testing.T) {
	// Verify that realHdfsClient embeds *hdfs.Client properly.
	// We can only check the type satisfies the interface; calling methods requires HDFS.
	var c hdfsClientIface = &realHdfsClient{}
	require.NotNil(t, c)
}

// TestNewHdfsFileReader_InvalidHost verifies the error path when HDFS is unreachable.
// The hdfs library may return an error immediately for invalid hosts.
func TestNewHdfsFileReader_InvalidHost(t *testing.T) {
	_, err := NewHdfsFileReader([]string{"invalid-host:9999"}, "user", "/file.parquet")
	require.Error(t, err)
}

// TestNewHdfsFileWriter_InvalidHost verifies the error path when HDFS is unreachable.
func TestNewHdfsFileWriter_InvalidHost(t *testing.T) {
	_, err := NewHdfsFileWriter([]string{"invalid-host:9999"}, "user", "/file.parquet")
	require.Error(t, err)
}
