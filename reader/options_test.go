package reader

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
)

func TestApplyReaderDefaults_Defaults(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	require.NoError(t, applyReaderDefaults(pr, nil))
	require.Equal(t, int64(4), pr.np)
	require.False(t, pr.caseInsensitive)
}

func TestApplyReaderDefaults_AppliesOptions(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	opts := []ReaderOption{
		WithNP(8),
		WithCaseInsensitive(true),
		WithCRCMode(common.CRCStrict),
		WithFooterKey([]byte("footer-key")),
		WithAADPrefix([]byte("aad")),
		WithColumnKey("a.b", []byte("col-key")),
	}
	require.NoError(t, applyReaderDefaults(pr, opts))

	require.Equal(t, int64(8), pr.np)
	require.True(t, pr.caseInsensitive)
	require.Equal(t, common.CRCStrict, pr.crcMode)
	require.Equal(t, []byte("footer-key"), pr.footerKey)
	require.Equal(t, []byte("aad"), pr.aadPrefix)
	require.Equal(t, []byte("col-key"), pr.columnKeys[common.ReformPathStr("a.b")])
}

func TestApplyReaderDefaults_InvalidNP(t *testing.T) {
	t.Parallel()

	for _, np := range []int64{0, -1} {
		pr := &ParquetReader{}
		err := applyReaderDefaults(pr, []ReaderOption{WithNP(np)})
		require.Error(t, err)
		require.Contains(t, err.Error(), "WithNP: value must be positive")
	}
}

func TestApplyReaderDefaults_InvalidCRCMode(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	err := applyReaderDefaults(pr, []ReaderOption{WithCRCMode(common.CRCMode(99))})
	require.Error(t, err)
	require.Contains(t, err.Error(), "WithCRCMode: unsupported mode")
}

func TestApplyReaderDefaults_ValidCRCModes(t *testing.T) {
	t.Parallel()

	for _, mode := range []common.CRCMode{common.CRCIgnore, common.CRCAuto, common.CRCStrict} {
		pr := &ParquetReader{}
		require.NoError(t, applyReaderDefaults(pr, []ReaderOption{WithCRCMode(mode)}))
		require.Equal(t, mode, pr.crcMode)
	}
}

func TestWithKeyRetriever(t *testing.T) {
	t.Parallel()

	called := false
	retriever := func(keyMetadata []byte) ([]byte, error) {
		called = true
		return []byte("resolved"), nil
	}

	pr := &ParquetReader{}
	require.NoError(t, applyReaderDefaults(pr, []ReaderOption{WithKeyRetriever(retriever)}))
	require.NotNil(t, pr.keyRetriever)

	key, err := pr.keyRetriever([]byte("meta"))
	require.NoError(t, err)
	require.Equal(t, []byte("resolved"), key)
	require.True(t, called)
}

func TestWithColumnKey_MultipleColumns(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	opts := []ReaderOption{
		WithColumnKey("a.b", []byte("key1")),
		WithColumnKey("c.d", []byte("key2")),
	}
	require.NoError(t, applyReaderDefaults(pr, opts))
	require.Len(t, pr.columnKeys, 2)
	require.Equal(t, []byte("key1"), pr.columnKeys[common.ReformPathStr("a.b")])
	require.Equal(t, []byte("key2"), pr.columnKeys[common.ReformPathStr("c.d")])
}

func TestWithFooterKey_CopiesInput(t *testing.T) {
	t.Parallel()

	key := []byte("secret")
	pr := &ParquetReader{}
	require.NoError(t, applyReaderDefaults(pr, []ReaderOption{WithFooterKey(key)}))

	// Mutating the caller's slice must not affect the stored key.
	key[0] = 'X'
	require.Equal(t, []byte("secret"), pr.footerKey)
}
