package encryption

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeModule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		buf     []byte
		want    []byte
		wantErr string
	}{
		{
			name: "valid",
			buf: func() []byte {
				buf := make([]byte, 4)
				binary.LittleEndian.PutUint32(buf, 3)
				return append(buf, []byte("abc")...)
			}(),
			want: []byte("abc"),
		},
		{name: "short length", buf: []byte{1, 2}, wantErr: "too short"},
		{
			name: "declared length too large",
			buf: func() []byte {
				buf := make([]byte, 4)
				binary.LittleEndian.PutUint32(buf, 4)
				return append(buf, []byte("abc")...)
			}(),
			wantErr: "exceeds available bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := DecodeModule(tt.buf)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestReadModule(t *testing.T) {
	t.Parallel()

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, 3)
	buf = append(buf, []byte("abc")...)

	got, err := ReadModule(bytes.NewReader(buf), 10)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), got)

	_, err = ReadModule(bytes.NewReader(buf), 2)
	require.ErrorContains(t, err, "exceeds limit")

	_, err = ReadModule(bytes.NewReader([]byte{1, 2}), 0)
	require.ErrorContains(t, err, "read encrypted module length")

	truncated := make([]byte, 4)
	binary.LittleEndian.PutUint32(truncated, 4)
	truncated = append(truncated, []byte("abc")...)
	_, err = ReadModule(bytes.NewReader(truncated), 0)
	require.ErrorContains(t, err, "read encrypted module body")
}

func TestDecryptGCM(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	nonce := []byte("123456789012")
	aad := AAD([]byte("prefix"), []byte("file-id"), ModuleFooter, 0, 0, 0)
	plaintext := []byte("footer payload")

	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCMWithNonceSize(block, 12)
	require.NoError(t, err)
	module := append(append([]byte{}, nonce...), gcm.Seal(nil, nonce, plaintext, aad)...)

	got, err := DecryptGCM(key, aad, module)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)

	_, err = DecryptGCM(key, []byte("wrong aad"), module)
	require.ErrorContains(t, err, "decrypt AES-GCM module")

	err = VerifyGCMTag(key, aad, nonce, plaintext, module[len(module)-gcmTagSize:])
	require.NoError(t, err)

	err = VerifyGCMTag(key, []byte("wrong aad"), nonce, plaintext, module[len(module)-gcmTagSize:])
	require.ErrorContains(t, err, "tag verification failed")
}

func TestDecryptGCMErrors(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	_, err := DecryptGCM(key, nil, make([]byte, gcmNonceSize+gcmTagSize-1))
	require.ErrorContains(t, err, "AES-GCM module too short")
}

func TestVerifyGCMTagErrors(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	nonce := []byte("123456789012")
	plaintext := []byte("footer payload")
	aad := []byte("aad")

	err := VerifyGCMTag(key, aad, nonce[:len(nonce)-1], plaintext, make([]byte, gcmTagSize))
	require.ErrorContains(t, err, "invalid AES-GCM nonce size")

	err = VerifyGCMTag(key, aad, nonce, plaintext, make([]byte, gcmTagSize-1))
	require.ErrorContains(t, err, "invalid AES-GCM tag size")

	err = VerifyGCMTag([]byte("bad"), aad, nonce, plaintext, make([]byte, gcmTagSize))
	require.ErrorContains(t, err, "invalid AES key size")
}

func TestDecryptCTR(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	nonce := []byte("123456789012")
	plaintext := []byte("page payload")

	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	iv := make([]byte, aes.BlockSize)
	copy(iv, nonce)
	iv[aes.BlockSize-1] = 1
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCTR(block, iv).XORKeyStream(ciphertext, plaintext)

	module := append(append([]byte{}, nonce...), ciphertext...)
	got, err := DecryptCTR(key, module)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestDecryptCTRErrors(t *testing.T) {
	t.Parallel()

	_, err := DecryptCTR([]byte("0123456789abcdef"), make([]byte, ctrNonceSize-1))
	require.ErrorContains(t, err, "AES-CTR module too short")
}

func TestInvalidAESKeySize(t *testing.T) {
	t.Parallel()

	_, err := DecryptGCM([]byte("bad"), nil, make([]byte, gcmNonceSize+gcmTagSize))
	require.ErrorContains(t, err, "invalid AES key size")

	_, err = DecryptCTR([]byte("bad"), make([]byte, ctrNonceSize))
	require.ErrorContains(t, err, "invalid AES key size")
}

func TestNewAESBlockValidKeySizes(t *testing.T) {
	t.Parallel()

	for _, size := range []int{16, 24, 32} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			_, err := newAESBlock(bytes.Repeat([]byte{0x42}, size))
			require.NoError(t, err)
		})
	}
}
