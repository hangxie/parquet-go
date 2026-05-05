package reader

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source/local"
)

const (
	encryptedFooterFile = "../build/testdata/encrypt_columns_and_footer.parquet.encrypted"
	plaintextFooterFile = "../build/testdata/encrypt_columns_plaintext_footer.parquet.encrypted"
	aadDisabledFile     = "../build/testdata/encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted"
	ctrFile             = "../build/testdata/encrypt_columns_and_footer_ctr.parquet.encrypted"
)

var encryptionKeys = map[string][]byte{
	"kf":  []byte("0123456789012345"),
	"kc1": []byte("1234567890123450"),
	"kc2": []byte("1234567890123451"),
}

func retrieveEncryptionKey(keyMetadata []byte) ([]byte, error) {
	id := string(keyMetadata)
	if id == "" {
		id = "kf"
	}
	key, ok := encryptionKeys[id]
	if !ok {
		return nil, fmt.Errorf("unknown key ID: %q", id)
	}
	return key, nil
}

func encryptedTestdataAvailable(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(encryptedFooterFile); os.IsNotExist(err) {
		t.Skip("encrypted test data not available, run 'make testdata' first")
	}
}

func TestInteropEncryptedFooter(t *testing.T) {
	t.Parallel()
	encryptedTestdataAvailable(t)

	pf, err := local.NewLocalFileReader(encryptedFooterFile)
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()

	pr, err := NewParquetReader(pf, nil, WithKeyRetriever(retrieveEncryptionKey))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	require.Positive(t, pr.GetNumRows())
	rows, err := pr.ReadByNumber(int(pr.GetNumRows()))
	require.NoError(t, err)
	require.NotEmpty(t, rows)
}

func TestInteropEncryptedFooter_WrongKey(t *testing.T) {
	t.Parallel()
	encryptedTestdataAvailable(t)

	pf, err := local.NewLocalFileReader(encryptedFooterFile)
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()

	wrongKey := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	_, err = NewParquetReader(pf, nil, WithFooterKey(wrongKey))
	require.ErrorContains(t, err, "read footer")
}

func TestInteropPlaintextFooter(t *testing.T) {
	t.Parallel()
	encryptedTestdataAvailable(t)

	pf, err := local.NewLocalFileReader(plaintextFooterFile)
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()

	pr, err := NewParquetReader(
		pf, nil,
		WithFooterKey(encryptionKeys["kf"]),
		WithColumnKey("double_field", encryptionKeys["kc1"]),
		WithColumnKey("float_field", encryptionKeys["kc2"]),
	)
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	require.Positive(t, pr.GetNumRows())
	rows, err := pr.ReadByNumber(int(pr.GetNumRows()))
	require.NoError(t, err)
	require.NotEmpty(t, rows)
}

func TestInteropPlaintextFooter_WrongFooterKey(t *testing.T) {
	t.Parallel()
	encryptedTestdataAvailable(t)

	pf, err := local.NewLocalFileReader(plaintextFooterFile)
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()

	wrongKey := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	_, err = NewParquetReader(
		pf, nil,
		WithFooterKey(wrongKey),
		WithColumnKey("double_field", encryptionKeys["kc1"]),
		WithColumnKey("float_field", encryptionKeys["kc2"]),
	)
	require.ErrorContains(t, err, "read footer")
}

func TestInteropAADPrefix(t *testing.T) {
	t.Parallel()
	encryptedTestdataAvailable(t)

	pf, err := local.NewLocalFileReader(aadDisabledFile)
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()

	pr, err := NewParquetReader(
		pf, nil,
		WithKeyRetriever(retrieveEncryptionKey),
		WithAADPrefix([]byte("tester")),
	)
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	require.Positive(t, pr.GetNumRows())
	rows, err := pr.ReadByNumber(int(pr.GetNumRows()))
	require.NoError(t, err)
	require.NotEmpty(t, rows)
}

func TestInteropAADPrefix_MissingAAD(t *testing.T) {
	t.Parallel()
	encryptedTestdataAvailable(t)

	pf, err := local.NewLocalFileReader(aadDisabledFile)
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()

	_, err = NewParquetReader(pf, nil, WithKeyRetriever(retrieveEncryptionKey))
	require.ErrorContains(t, err, "AAD prefix is required")
}

func TestInteropCTRMode(t *testing.T) {
	t.Parallel()
	encryptedTestdataAvailable(t)

	pf, err := local.NewLocalFileReader(ctrFile)
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()

	pr, err := NewParquetReader(pf, nil, WithKeyRetriever(retrieveEncryptionKey))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	require.Positive(t, pr.GetNumRows())
	rows, err := pr.ReadByNumber(int(pr.GetNumRows()))
	require.NoError(t, err)
	require.NotEmpty(t, rows)
}
