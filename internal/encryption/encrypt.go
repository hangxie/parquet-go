package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
)

// EncryptGCM encrypts a Parquet AES-GCM module body as nonce, ciphertext, and
// authentication tag.
func EncryptGCM(key, aad, plaintext []byte) ([]byte, error) {
	gcm, err := newGCMCipher(key)
	if err != nil {
		return nil, fmt.Errorf("init AES-GCM cipher: %w", err)
	}
	nonce, err := randomNonce(gcmNonceSize)
	if err != nil {
		return nil, fmt.Errorf("GCM nonce: %w", err)
	}
	module := append([]byte{}, nonce...)
	module = gcm.Seal(module, nonce, plaintext, aad)
	return module, nil
}

// SignGCM returns the nonce and authentication tag for a plaintext-footer
// signature. The plaintext itself is not included in the returned module.
func SignGCM(key, aad, plaintext []byte) ([]byte, error) {
	gcm, err := newGCMCipher(key)
	if err != nil {
		return nil, fmt.Errorf("init AES-GCM cipher: %w", err)
	}
	nonce, err := randomNonce(gcmNonceSize)
	if err != nil {
		return nil, fmt.Errorf("GCM nonce: %w", err)
	}
	// gcm.Seal encrypts the full plaintext; only the appended tag is kept.
	// Go's cipher.AEAD interface has no tag-only (GHASH-only) path, so the
	// ciphertext allocation is unavoidable with the standard library.
	sealed := gcm.Seal(nil, nonce, plaintext, aad)
	sig := make([]byte, 0, gcmNonceSize+gcmTagSize)
	sig = append(sig, nonce...)
	sig = append(sig, sealed[len(sealed)-gcmTagSize:]...)
	return sig, nil
}

// EncryptCTR encrypts a Parquet AES-CTR page module body as nonce and
// ciphertext. Per the Parquet AES-GCM-CTR spec, the AES IV is the 12-byte
// random nonce followed by a 4-byte big-endian counter initialized to 1.
func EncryptCTR(key, plaintext []byte) ([]byte, error) {
	block, err := newAESBlock(key)
	if err != nil {
		return nil, fmt.Errorf("init AES block: %w", err)
	}
	nonce, err := randomNonce(ctrNonceSize)
	if err != nil {
		return nil, fmt.Errorf("CTR nonce: %w", err)
	}
	iv := make([]byte, aes.BlockSize)
	copy(iv, nonce)
	binary.BigEndian.PutUint32(iv[12:], 1) // 4-byte big-endian counter initialized to 1

	ciphertext := make([]byte, len(plaintext))
	cipher.NewCTR(block, iv).XORKeyStream(ciphertext, plaintext)
	module := append([]byte{}, nonce...)
	module = append(module, ciphertext...)
	return module, nil
}

// EncodeModule wraps an encrypted Parquet module with a 4-byte little-endian
// body length prefix.
func EncodeModule(module []byte) ([]byte, error) {
	buf := make([]byte, lengthSize, lengthSize+len(module))
	binary.LittleEndian.PutUint32(buf, uint32(len(module)))
	return append(buf, module...), nil
}

func randomNonce(size int) ([]byte, error) {
	nonce := make([]byte, size)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate encryption nonce: %w", err)
	}
	return nonce, nil
}
