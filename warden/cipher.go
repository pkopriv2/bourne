package warden

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"io"
	"reflect"

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

type ASymCipher int32

const (
	RSA_WITH_SHA1 ASymCipher = iota
	RSA_WITH_SHA256
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

type aSymCipherText struct {
	KeyCipher ASymCipher
	Key       []byte
	Msg       symCipherText
}

func ASymmetricEncrypt(keyCipher ASymCipher, msgCipher SymCipher, pub crypto.PublicKey, msg []byte) (aSymCipherText, error) {
	key, err := initRandomSymmetricKey(msgCipher)
	if err != nil {
		return aSymCipherText{}, errors.WithStack(err)
	}

	encKey, err := encryptKey(keyCipher, pub, key)
	if err != nil {
		return aSymCipherText{}, errors.WithStack(err)
	}

	encMsg, err := Encrypt(msgCipher, key, msg)
	if err != nil {
		return aSymCipherText{}, errors.WithStack(err)
	}

	return aSymCipherText{keyCipher, encKey, encMsg}, nil
}

func (c aSymCipherText) Decrypt(priv crypto.PrivateKey) ([]byte, error) {
	key, err := decryptKey(c.KeyCipher, priv, c.Key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.Msg.Decrypt(key)
}

const (
	BITS_128 = 128 / 8
	BITS_192 = 192 / 8
	BITS_256 = 256 / 8
)

func encryptKey(alg ASymCipher, raw crypto.PublicKey, key []byte) ([]byte, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(CipherUnknownError, "Unknown asymmetric cipher: %v", alg)
	case RSA_WITH_SHA1:
		pub, ok := raw.(*rsa.PublicKey)
		if !ok {
			return nil, errors.Wrapf(CipherKeyError, "Expected *rsa.PublicKey. Not [%v]", reflect.TypeOf(raw))
		}

		return rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, key, []byte{})
	case RSA_WITH_SHA256:
		pub, ok := raw.(*rsa.PublicKey)
		if !ok {
			return nil, errors.Wrapf(CipherKeyError, "Expected *rsa.PublicKey. Not [%v]", reflect.TypeOf(raw))
		}
		return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, key, []byte{})
	}
}

func decryptKey(alg ASymCipher, raw crypto.PrivateKey, key []byte) ([]byte, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(CipherUnknownError, "Unknown asymmetric cipher: %v", alg)
	case RSA_WITH_SHA1:
		priv, ok := raw.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.Wrapf(CipherKeyError, "Expected *rsa.PrivateKey. Not [%v]", reflect.TypeOf(raw))
		}

		return rsa.DecryptOAEP(sha1.New(), rand.Reader, priv, key, []byte{})
	case RSA_WITH_SHA256:
		priv, ok := raw.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.Wrapf(CipherKeyError, "Expected *rsa.PrivateKey. Not [%v]", reflect.TypeOf(raw))
		}

		return rsa.DecryptOAEP(sha1.New(), rand.Reader, priv, key, []byte{})
	}
}

func initRandomSymmetricKey(alg SymCipher) ([]byte, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(CipherUnknownError, "Unknown cipher: %v", alg)
	case AES_128_GCM:
		return generateRandomBytes(BITS_128)
	case AES_192_GCM:
		return generateRandomBytes(BITS_192)
	case AES_256_GCM:
		return generateRandomBytes(BITS_256)
	}
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
		return ensureKeySize(BITS_128, key)
	case AES_192_GCM:
		return ensureKeySize(BITS_192, key)
	case AES_256_GCM:
		return ensureKeySize(BITS_256, key)
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
