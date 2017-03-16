package warden

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	"github.com/pkg/errors"
)

var (
	CipherUnknownError = errors.New("WARDEN:UNKNOWN_CIPHER")
	CipherKeyError     = errors.New("WARDEN:KEY_ERROR")
)

// Supported symmetric ciphers.  This library is intended to ONLY offer support ciphers
// that implement the Authenticated Encryption with Associated Data (AEAD)
// standard.  Currently, that only includes the GCM family of streaming modes.
type SymCipher int32

const (
	AES_128_GCM SymCipher = iota
	AES_192_GCM
	AES_256_GCM
)

const (
	BYTES_128_BITS = 128 / 8
	BYTES_192_BITS = 192 / 8
	BYTES_256_BITS = 256 / 8
)

func newCipherKeyLengthError(expected int, key []byte) error {
	return errors.Wrapf(CipherKeyError, "Illegal key [%v].  Expected [%v] bytes but got [%v]", key, expected, len(key))
}

// TODO: Determine general set of fields for non-AE modes
//
// Currently thinking:
//  * Mac
type symCipherText struct {
	Cipher SymCipher
	Nonce  []byte
	Data   []byte
}

func Encrypt(alg SymCipher, key []byte, msg []byte) (symCipherText, error) {
	block, err := initBlockCipher(alg, key)
	if err != nil {
		return symCipherText{}, errors.WithStack(err)
	}

	strm, err := initStreamCipher(alg, block)
	if err != nil {
		return symCipherText{}, errors.WithStack(err)
	}

	nonce, err := generateNonce(strm.NonceSize())
	if err != nil {
		return symCipherText{}, errors.Wrapf(err, "Error generating nonce of [%v] bytes", strm.NonceSize())
	}

	return symCipherText{alg, nonce, strm.Seal(nil, nonce, msg, nil)}, nil
}

func (c symCipherText) Decrypt(key []byte) ([]byte, error) {
	block, err := initBlockCipher(c.Cipher, key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	strm, err := initStreamCipher(c.Cipher, block)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, err := strm.Open(nil, c.Nonce, c.Data, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return ret, nil
}

func initBlockCipher(alg SymCipher, key []byte) (cipher.Block, error) {
	if err := ensureValidKey(alg, key); err != nil {
		return nil, errors.WithStack(err)
	}

	switch alg {
	default:
		return nil, errors.Wrapf(CipherUnknownError, "Unknown cipher: %v", alg)
	case AES_128_GCM, AES_192_GCM, AES_256_GCM:
		return aes.NewCipher(key)
	}
}

func initStreamCipher(alg SymCipher, blk cipher.Block) (cipher.AEAD, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(CipherUnknownError, "Unknown cipher: %v", alg)
	case AES_128_GCM, AES_192_GCM, AES_256_GCM:
		return cipher.NewGCM(blk)
	}
}

// Creates a new random nonce.  Nonces are essentially the same
// thing as initialization vectors and should be use
func ensureValidKey(alg SymCipher, key []byte) error {
	switch alg {
	default:
		return errors.Wrapf(CipherUnknownError, "Unknown cipher: %v", alg)
	case AES_128_GCM:
		return ensureKeySize(BYTES_128_BITS, key)
	case AES_192_GCM:
		return ensureKeySize(BYTES_192_BITS, key)
	case AES_256_GCM:
		return ensureKeySize(BYTES_256_BITS, key)
	}
}

func ensureKeySize(expected int, key []byte) error {
	if expected != len(key) {
		return newCipherKeyLengthError(expected, key)
	} else {
		return nil
	}
}

// Creates a new random nonce.  Nonces are essentially the same
// thing as initialization vectors and should be use
func generateNonce(size int) ([]byte, error) {
	return generateRandomBytes(size)
}

// Generates some random bytes (this should be considered )
func generateRandomBytes(size int) ([]byte, error) {
	arr := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, arr); err != nil {
		return nil, errors.WithStack(err)
	}
	return arr, nil
}
