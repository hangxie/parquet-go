package encryption

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryptGCMRoundTrip(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aad := AAD([]byte("prefix"), []byte("file-id"), ModuleDataPage, 0, 1, 2)
	plaintext := []byte("page payload data")

	module, err := EncryptGCM(key, aad, plaintext)
	require.NoError(t, err)
	require.Equal(t, gcmNonceSize+len(plaintext)+gcmTagSize, len(module))

	got, err := DecryptGCM(key, aad, module)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)

	_, err = DecryptGCM([]byte("abcdef0123456789"), aad, module)
	require.Error(t, err)

	_, err = DecryptGCM(key, []byte("wrong-aad"), module)
	require.Error(t, err)
}

func TestSignGCMVerify(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aad := AAD([]byte("prefix"), []byte("file-id"), ModuleFooter, 0, 0, 0)
	plaintext := []byte("footer plaintext")

	sig, err := SignGCM(key, aad, plaintext)
	require.NoError(t, err)
	require.Equal(t, gcmNonceSize+gcmTagSize, len(sig))

	nonce, tag := sig[:gcmNonceSize], sig[gcmNonceSize:]
	require.NoError(t, VerifyGCMTag(key, aad, nonce, plaintext, tag))

	require.Error(t, VerifyGCMTag([]byte("abcdef0123456789"), aad, nonce, plaintext, tag))
	require.Error(t, VerifyGCMTag(key, []byte("wrong-aad"), nonce, plaintext, tag))
}

func TestEncryptCTRRoundTrip(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	plaintext := []byte("page data payload")

	module, err := EncryptCTR(key, plaintext)
	require.NoError(t, err)
	require.Equal(t, ctrNonceSize+len(plaintext), len(module))

	got, err := DecryptCTR(key, module)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptInvalidKey(t *testing.T) {
	t.Parallel()

	invalidKey := []byte("short") // not 16, 24, or 32 bytes

	tests := []struct {
		name string
		fn   func() error
	}{
		{
			name: "EncryptGCM",
			fn:   func() error { _, err := EncryptGCM(invalidKey, nil, []byte("data")); return err },
		},
		{
			name: "SignGCM",
			fn:   func() error { _, err := SignGCM(invalidKey, nil, []byte("data")); return err },
		},
		{
			name: "EncryptCTR",
			fn:   func() error { _, err := EncryptCTR(invalidKey, []byte("data")); return err },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorContains(t, tt.fn(), "invalid AES key size")
		})
	}
}

func TestEncryptRoundTripAllKeySizes(t *testing.T) {
	t.Parallel()

	keys := [][]byte{
		[]byte("0123456789abcdef"),                 // AES-128
		[]byte("0123456789abcdef01234567"),         // AES-192
		[]byte("0123456789abcdef0123456789abcdef"), // AES-256
	}
	aad := AAD([]byte("prefix"), []byte("file-id"), ModuleDataPage, 0, 1, 2)
	plaintext := []byte("page payload data")

	for _, key := range keys {
		t.Run("GCM", func(t *testing.T) {
			t.Parallel()
			module, err := EncryptGCM(key, aad, plaintext)
			require.NoError(t, err)
			got, err := DecryptGCM(key, aad, module)
			require.NoError(t, err)
			require.Equal(t, plaintext, got)
		})
		t.Run("CTR", func(t *testing.T) {
			t.Parallel()
			module, err := EncryptCTR(key, plaintext)
			require.NoError(t, err)
			got, err := DecryptCTR(key, module)
			require.NoError(t, err)
			require.Equal(t, plaintext, got)
		})
		t.Run("Sign", func(t *testing.T) {
			t.Parallel()
			sig, err := SignGCM(key, aad, plaintext)
			require.NoError(t, err)
			require.Equal(t, gcmNonceSize+gcmTagSize, len(sig))
			require.NoError(t, VerifyGCMTag(key, aad, sig[:gcmNonceSize], plaintext, sig[gcmNonceSize:]))
		})
	}
}

func TestEncryptLargePayload(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aad := AAD([]byte("prefix"), []byte("file-id"), ModuleDataPage, 0, 0, 0)
	plaintext := bytes.Repeat([]byte("A"), 1024*1024) // 1 MiB

	t.Run("GCM", func(t *testing.T) {
		t.Parallel()
		module, err := EncryptGCM(key, aad, plaintext)
		require.NoError(t, err)
		got, err := DecryptGCM(key, aad, module)
		require.NoError(t, err)
		require.Equal(t, plaintext, got)
	})

	t.Run("CTR", func(t *testing.T) {
		t.Parallel()
		module, err := EncryptCTR(key, plaintext)
		require.NoError(t, err)
		got, err := DecryptCTR(key, module)
		require.NoError(t, err)
		require.Equal(t, plaintext, got)
	})
}

func TestEncodeDecodeModuleRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "normal payload", payload: []byte("encrypted content")},
		{name: "empty payload", payload: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := EncodeModule(tt.payload)
			require.NoError(t, err)
			require.Equal(t, lengthSize+len(tt.payload), len(encoded))

			got, err := DecodeModule(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.payload, got)
		})
	}
}
