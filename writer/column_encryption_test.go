package writer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// applyOptionsForTest applies WriterOption values to a fresh ParquetWriter
// and returns the resulting encryption config plus any option-time errors.
// It exercises only the option-application path; it does not call
// NewParquetWriter and so does not touch the filesystem, schema, or
// encryption-state validation.
func applyOptionsForTest(opts ...WriterOption) (*EncryptionConfig, []error) {
	pw := &ParquetWriter{}
	for _, opt := range opts {
		opt.apply(pw)
	}
	return pw.encryptionConfig, pw.optionErrors
}

func TestColumnFooterKeyOption(t *testing.T) {
	t.Parallel()

	cfg, errs := applyOptionsForTest(
		WithColumnEncrypted("name"),
		WithColumnEncrypted("score", ColumnFooterKey()),
	)
	require.Empty(t, errs)
	require.NotNil(t, cfg)
	require.Equal(t, EncryptionColumnKey{}, cfg.ColumnKeys[common.ReformPathStr("name")])
	require.Equal(t, EncryptionColumnKey{}, cfg.ColumnKeys[common.ReformPathStr("score")])
}

func TestWithColumnEncryptedRejectsEmptyPath(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"", "   "} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			cfg, errs := applyOptionsForTest(WithColumnEncrypted(path))
			require.Nil(t, cfg)
			require.Len(t, errs, 1)
			require.ErrorContains(t, errs[0], "WithColumnEncrypted: path must be non-empty")
		})
	}
}

func TestColumnKeyOption(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	kmd := []byte("name-key")

	t.Run("key only", func(t *testing.T) {
		t.Parallel()
		cfg, errs := applyOptionsForTest(WithColumnEncrypted("name", ColumnKey(key)))
		require.Empty(t, errs)
		entry := cfg.ColumnKeys[common.ReformPathStr("name")]
		require.Equal(t, key, entry.Key)
		require.Empty(t, entry.KeyMetadata)
	})

	t.Run("key with stored metadata", func(t *testing.T) {
		t.Parallel()
		cfg, errs := applyOptionsForTest(WithColumnEncrypted("name", ColumnKey(key, kmd)))
		require.Empty(t, errs)
		entry := cfg.ColumnKeys[common.ReformPathStr("name")]
		require.Equal(t, key, entry.Key)
		require.Equal(t, kmd, entry.KeyMetadata)
	})

	t.Run("nil key rejected", func(t *testing.T) {
		t.Parallel()
		_, errs := applyOptionsForTest(WithColumnEncrypted("name", ColumnKey(nil)))
		require.Len(t, errs, 1)
		require.ErrorContains(t, errs[0], `WithColumnEncrypted "name"`)
		require.ErrorContains(t, errs[0], `ColumnKey: key must not be nil`)
	})

	t.Run("two metadata args rejected", func(t *testing.T) {
		t.Parallel()
		_, errs := applyOptionsForTest(WithColumnEncrypted("name", ColumnKey(key, kmd, kmd)))
		require.Len(t, errs, 1)
		require.ErrorContains(t, errs[0], `WithColumnEncrypted "name"`)
		require.ErrorContains(t, errs[0], "at most one keyMetadata argument accepted, got 2")
	})
}

func TestColumnKeyByMetadataOption(t *testing.T) {
	t.Parallel()

	kmd := []byte("name-key")

	t.Run("valid metadata", func(t *testing.T) {
		t.Parallel()
		cfg, errs := applyOptionsForTest(WithColumnEncrypted("name", ColumnKeyByMetadata(kmd)))
		require.Empty(t, errs)
		entry := cfg.ColumnKeys[common.ReformPathStr("name")]
		require.Empty(t, entry.Key)
		require.Equal(t, kmd, entry.KeyMetadata)
	})

	t.Run("nil metadata rejected", func(t *testing.T) {
		t.Parallel()
		_, errs := applyOptionsForTest(WithColumnEncrypted("name", ColumnKeyByMetadata(nil)))
		require.Len(t, errs, 1)
		require.ErrorContains(t, errs[0], `WithColumnEncrypted "name"`)
		require.ErrorContains(t, errs[0], "ColumnKeyByMetadata: keyMetadata must be non-empty")
	})

	t.Run("empty metadata rejected", func(t *testing.T) {
		t.Parallel()
		_, errs := applyOptionsForTest(WithColumnEncrypted("name", ColumnKeyByMetadata([]byte{})))
		require.Len(t, errs, 1)
		require.ErrorContains(t, errs[0], "ColumnKeyByMetadata: keyMetadata must be non-empty")
	})
}

func TestWithColumnEncryptedMultipleSubOptions(t *testing.T) {
	t.Parallel()

	_, errs := applyOptionsForTest(
		WithColumnEncrypted("name", ColumnFooterKey(), ColumnKey([]byte("0123456789abcdef"))),
	)
	require.Len(t, errs, 1)
	require.ErrorContains(t, errs[0], `WithColumnEncrypted "name"`)
	require.ErrorContains(t, errs[0], "at most one sub-option allowed, got 2")
}

func TestWithColumnEncryptedLastCallWins(t *testing.T) {
	t.Parallel()

	key1 := []byte("0123456789abcdef")
	key2 := []byte("abcdef0123456789")

	t.Run("footer-key then column-key", func(t *testing.T) {
		t.Parallel()
		cfg, errs := applyOptionsForTest(
			WithColumnEncrypted("name"),
			WithColumnEncrypted("name", ColumnKey(key2)),
		)
		require.Empty(t, errs)
		entry := cfg.ColumnKeys[common.ReformPathStr("name")]
		require.Equal(t, key2, entry.Key)
	})

	t.Run("column-key then footer-key", func(t *testing.T) {
		t.Parallel()
		cfg, errs := applyOptionsForTest(
			WithColumnEncrypted("name", ColumnKey(key1)),
			WithColumnEncrypted("name"),
		)
		require.Empty(t, errs)
		entry := cfg.ColumnKeys[common.ReformPathStr("name")]
		require.Equal(t, EncryptionColumnKey{}, entry)
	})

	t.Run("two column-keys", func(t *testing.T) {
		t.Parallel()
		cfg, errs := applyOptionsForTest(
			WithColumnEncrypted("name", ColumnKey(key1)),
			WithColumnEncrypted("name", ColumnKey(key2)),
		)
		require.Empty(t, errs)
		entry := cfg.ColumnKeys[common.ReformPathStr("name")]
		require.Equal(t, key2, entry.Key)
	})
}

func TestStructLiteralFooterKeySentinel(t *testing.T) {
	t.Parallel()

	// User builds the map literally — the zero EncryptionColumnKey value
	// must produce footer-key encryption, mirroring WithColumnEncrypted(p).
	cfg := EncryptionConfig{
		FooterKey: []byte("0123456789abcdef"),
		ColumnKeys: map[string]EncryptionColumnKey{
			common.ReformPathStr("name"): {},
		},
	}
	state, err := newEncryptionState(cfg)
	require.NoError(t, err)
	require.Contains(t, state.columnKeys, common.ReformPathStr("name"))
	require.Equal(t, EncryptionColumnKey{}, state.columnKeys[common.ReformPathStr("name")])
}

func TestNewEncryptionStateAcceptsFooterKeySentinel(t *testing.T) {
	t.Parallel()

	cfg := EncryptionConfig{
		FooterKey: []byte("0123456789abcdef"),
		ColumnKeys: map[string]EncryptionColumnKey{
			common.ReformPathStr("name"): {}, // footer-key sentinel
		},
	}
	state, err := newEncryptionState(cfg)
	require.NoError(t, err)
	require.Equal(t, EncryptionColumnKey{}, state.columnKeys[common.ReformPathStr("name")])
}

func TestNewEncryptionStateRejectsEmptyColumnRetrieverResult(t *testing.T) {
	t.Parallel()

	cfg := EncryptionConfig{
		FooterKey: []byte("0123456789abcdef"),
		ColumnKeys: map[string]EncryptionColumnKey{
			common.ReformPathStr("name"): {KeyMetadata: []byte("name-key")},
		},
		KeyRetriever: func([]byte) ([]byte, error) { return nil, nil },
	}
	_, err := newEncryptionState(cfg)
	require.Error(t, err)
	require.ErrorContains(t, err, `column "name" resolved to empty key`)
}

func TestNewEncryptionStatePropagatesColumnRetrieverError(t *testing.T) {
	t.Parallel()

	want := errors.New("kms unreachable")
	cfg := EncryptionConfig{
		FooterKey: []byte("0123456789abcdef"),
		ColumnKeys: map[string]EncryptionColumnKey{
			common.ReformPathStr("name"): {KeyMetadata: []byte("name-key")},
		},
		KeyRetriever: func([]byte) ([]byte, error) { return nil, want },
	}
	_, err := newEncryptionState(cfg)
	require.ErrorIs(t, err, want)
	require.ErrorContains(t, err, `retrieve column key "name"`)
}

func TestNewEncryptionStateRejectsByMetadataWithoutRetriever(t *testing.T) {
	t.Parallel()

	cfg := EncryptionConfig{
		FooterKey: []byte("0123456789abcdef"),
		ColumnKeys: map[string]EncryptionColumnKey{
			common.ReformPathStr("name"): {KeyMetadata: []byte("name-key")},
		},
		// No KeyRetriever.
	}
	_, err := newEncryptionState(cfg)
	require.Error(t, err)
	require.ErrorContains(t, err, `column "name" has KeyMetadata but no KeyRetriever`)
}

func TestNewEncryptionStateAcceptsColumnRetrieverSuccess(t *testing.T) {
	t.Parallel()

	resolved := []byte("abcdef0123456789")
	cfg := EncryptionConfig{
		FooterKey: []byte("0123456789abcdef"),
		ColumnKeys: map[string]EncryptionColumnKey{
			common.ReformPathStr("name"): {KeyMetadata: []byte("name-key")},
		},
		KeyRetriever: func(md []byte) ([]byte, error) {
			require.Equal(t, []byte("name-key"), md)
			return resolved, nil
		},
	}
	state, err := newEncryptionState(cfg)
	require.NoError(t, err)
	entry := state.columnKeys[common.ReformPathStr("name")]
	require.Equal(t, resolved, entry.Key)
	require.Equal(t, []byte("name-key"), entry.KeyMetadata)
}

func TestColumnKeyByMetadataWritesColumnCryptoMetadata(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	resolved := []byte("abcdef0123456789")
	keyMetadata := []byte("name-key")

	pw, _, err := createTestParquetWriter(
		new(encryptedWriterRecord),
		WithFooterKey(footerKey),
		WithColumnEncrypted("name", ColumnKeyByMetadata(keyMetadata)),
		WithKeyRetriever(func(md []byte) ([]byte, error) {
			require.Equal(t, keyMetadata, md)
			return resolved, nil
		}),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(encryptedWriterRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.WriteStop())

	rg := pw.Footer.RowGroups[0]
	var idCol, nameCol *parquet.ColumnChunk
	for _, c := range rg.Columns {
		switch c.MetaData.PathInSchema[len(c.MetaData.PathInSchema)-1] {
		case "ID", "id":
			idCol = c
		case "Name", "name":
			nameCol = c
		}
	}
	require.NotNil(t, idCol)
	require.NotNil(t, nameCol)
	require.Nil(t, idCol.CryptoMetadata)
	nameCrypto := nameCol.CryptoMetadata.GetENCRYPTION_WITH_COLUMN_KEY()
	require.NotNil(t, nameCrypto)
	require.Equal(t, keyMetadata, nameCrypto.GetKeyMetadata())
}

func TestOptionErrorsSurfaceFromNewParquetWriter(t *testing.T) {
	t.Parallel()

	_, _, err := createTestParquetWriter(
		new(encryptedWriterRecord),
		WithFooterKey([]byte("0123456789abcdef")),
		WithColumnEncrypted("name", ColumnKey(nil)),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, `WithColumnEncrypted "name"`)
	require.ErrorContains(t, err, "key must not be nil")
}

func TestOptionErrorsRenderOnOneLine(t *testing.T) {
	t.Parallel()

	_, _, err := createTestParquetWriter(
		new(encryptedWriterRecord),
		WithFooterKey([]byte("0123456789abcdef")),
		WithColumnEncrypted("name", ColumnKey(nil)),
		WithColumnEncrypted("score", ColumnKey(nil)),
	)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "\n")
	require.Contains(t, err.Error(), "; ")
	require.ErrorContains(t, err, `WithColumnEncrypted "name"`)
	require.ErrorContains(t, err, `WithColumnEncrypted "score"`)
}
