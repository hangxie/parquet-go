package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	gcmNonceSize = 12
	gcmTagSize   = 16
	ctrNonceSize = 12
	lengthSize   = 4
)

// DecodeModule unwraps an encrypted Parquet module encoded as a 4-byte
// little-endian length followed by the encrypted module bytes.
func DecodeModule(buf []byte) ([]byte, error) {
	if len(buf) < lengthSize {
		return nil, fmt.Errorf("encrypted module too short: %d", len(buf))
	}
	n := int(binary.LittleEndian.Uint32(buf[:lengthSize]))
	if n < 0 || len(buf)-lengthSize < n {
		return nil, fmt.Errorf("encrypted module length %d exceeds available bytes %d", n, len(buf)-lengthSize)
	}
	return buf[lengthSize : lengthSize+n], nil
}

// ReadModule reads a length-prefixed encrypted Parquet module from r and
// returns the module body without the 4-byte length prefix.
func ReadModule(r io.Reader, maxSize int64) ([]byte, error) {
	var lengthBuf [lengthSize]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return nil, fmt.Errorf("read encrypted module length: %w", err)
	}
	n := int64(binary.LittleEndian.Uint32(lengthBuf[:]))
	if maxSize > 0 && n > maxSize {
		return nil, fmt.Errorf("encrypted module size %d exceeds limit %d", n, maxSize)
	}
	module := make([]byte, n)
	if _, err := io.ReadFull(r, module); err != nil {
		return nil, fmt.Errorf("read encrypted module body: %w", err)
	}
	return module, nil
}

// DecryptGCM decrypts a Parquet AES-GCM module body encoded as nonce,
// ciphertext, and authentication tag.
func DecryptGCM(key, aad, module []byte) ([]byte, error) {
	if len(module) < gcmNonceSize+gcmTagSize {
		return nil, fmt.Errorf("AES-GCM module too short: %d", len(module))
	}
	block, err := newAESBlock(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, gcmNonceSize)
	if err != nil {
		return nil, fmt.Errorf("create AES-GCM: %w", err)
	}
	nonce := module[:gcmNonceSize]
	ciphertextAndTag := module[gcmNonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertextAndTag, aad)
	if err != nil {
		return nil, fmt.Errorf("decrypt AES-GCM module: %w", err)
	}
	return plaintext, nil
}

// VerifyGCMTag verifies a Parquet plaintext-footer signature. The signature
// stores only the GCM nonce and tag, while the footer plaintext remains
// serialized in the file.
func VerifyGCMTag(key, aad, nonce, plaintext, tag []byte) error {
	if len(nonce) != gcmNonceSize {
		return fmt.Errorf("invalid AES-GCM nonce size %d", len(nonce))
	}
	if len(tag) != gcmTagSize {
		return fmt.Errorf("invalid AES-GCM tag size %d", len(tag))
	}
	block, err := newAESBlock(key)
	if err != nil {
		return err
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, gcmNonceSize)
	if err != nil {
		return fmt.Errorf("create AES-GCM: %w", err)
	}
	sealed := gcm.Seal(nil, nonce, plaintext, aad)
	gotTag := sealed[len(sealed)-gcmTagSize:]
	if subtle.ConstantTimeCompare(gotTag, tag) != 1 {
		return fmt.Errorf("AES-GCM tag verification failed")
	}
	return nil
}

// DecryptCTR decrypts a Parquet AES-CTR page module body encoded as nonce and
// ciphertext. Parquet appends a 4-byte initial counter whose last bit is set.
func DecryptCTR(key, module []byte) ([]byte, error) {
	if len(module) < ctrNonceSize {
		return nil, fmt.Errorf("AES-CTR module too short: %d", len(module))
	}
	block, err := newAESBlock(key)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, aes.BlockSize)
	copy(iv, module[:ctrNonceSize])
	iv[aes.BlockSize-1] = 1

	ciphertext := module[ctrNonceSize:]
	plaintext := make([]byte, len(ciphertext))
	cipher.NewCTR(block, iv).XORKeyStream(plaintext, ciphertext)
	return plaintext, nil
}

func newAESBlock(key []byte) (cipher.Block, error) {
	switch len(key) {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("invalid AES key size %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	return block, nil
}
