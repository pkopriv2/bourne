package warden

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"reflect"

	"golang.org/x/crypto/pbkdf2"

	"github.com/pkg/errors"
)

var (
	CipherUnknownError = errors.New("WARDEN:UNKNOWN_CIPHER")
	CipherKeyError     = errors.New("WARDEN:KEY_ERROR")
)

// Supported symmetric ciphers.  This library is intended to ONLY offer support ciphers
// that implement the Authenticated Encryption with Associated Data (AEAD)
// standard.  Currently, that only includes the GCM family of streaming modes.

const (
	AES_128_GCM SymCipher = iota
	AES_192_GCM
	AES_256_GCM
)

// Symmetric Cipher Type.  (FIXME: Switches are getting annoying...)
type SymCipher int32

func (s SymCipher) KeySize() int {
	switch s {
	default:
		return 0
	case AES_128_GCM:
		return BITS_128
	case AES_192_GCM:
		return BITS_192
	case AES_256_GCM:
		return BITS_192
	}
}

func (s SymCipher) String() string {
	switch s {
	default:
		return "UnknownCipher"
	case AES_128_GCM:
		return "AES_128_GCM"
	case AES_192_GCM:
		return "AES_192_GCM"
	case AES_256_GCM:
		return "AES_256_GCM"
	}
}

// Supported asymmetric ciphers.
type ASymCipher int32

const (
	RSA_WITH_SHA1 ASymCipher = iota
	RSA_WITH_SHA256
)

// Simple byte decorator.
type Bytes []byte

func (e Bytes) Base64() string {
	return base64.StdEncoding.EncodeToString(e)
}

func newCipherKeyLengthError(expected int, key []byte) error {
	return errors.Wrapf(CipherKeyError, "Illegal key [%v].  Expected [%v] bytes but got [%v]", key, expected, len(key))
}

type secret struct {
	Raw []byte
}

func (s *secret) Destroy() {
	for i := 0; i < len(s.Raw); i++ {
		s.Raw[i] = 0
	}
}

func (s *secret) DeriveKey(salt []byte, size int) []byte {
	return pbkdf2.Key(s.Raw, salt, 4096, size, sha256.New)
}

// TODO: Determine general set of fields for non-AE modes
//
// Currently thinking:
//  * Mac
type symCipherText struct {
	Cipher SymCipher
	Nonce  Bytes
	Data   Bytes
}

func symmetricEncrypt(rand io.Reader, alg SymCipher, key []byte, msg []byte) (symCipherText, error) {
	block, err := initBlockCipher(alg, key)
	if err != nil {
		return symCipherText{}, errors.WithStack(err)
	}

	strm, err := initStreamCipher(alg, block)
	if err != nil {
		return symCipherText{}, errors.WithStack(err)
	}

	nonce, err := generateNonce(rand, strm.NonceSize())
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

func (c symCipherText) String() string {
	return fmt.Sprintf("SymmetricCipherText(alg=%v,nonce=%v): %v...", c.Cipher, c.Nonce.Base64(), c.Data.Base64())
}

type asymCipherText struct {
	KeyCipher ASymCipher
	Key       Bytes
	Msg       symCipherText
}

func asymmetricEncrypt(rand io.Reader, keyCipher ASymCipher, msgCipher SymCipher, pub crypto.PublicKey, msg []byte) (asymCipherText, error) {
	key, err := initRandomSymmetricKey(rand, msgCipher)
	if err != nil {
		return asymCipherText{}, errors.WithStack(err)
	}

	encKey, err := asymmetricEncryptKey(keyCipher, pub, key)
	if err != nil {
		return asymCipherText{}, errors.WithStack(err)
	}

	encMsg, err := symmetricEncrypt(rand, msgCipher, key, msg)
	if err != nil {
		return asymCipherText{}, errors.WithStack(err)
	}

	return asymCipherText{keyCipher, encKey, encMsg}, nil
}

func (c asymCipherText) Decrypt(priv crypto.PrivateKey) ([]byte, error) {
	key, err := decryptKey(c.KeyCipher, priv, c.Key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.Msg.Decrypt(key)
}

func (c asymCipherText) String() string {
	return fmt.Sprintf("AsymmetricCipherText(alg=%v,key=%v,val=%v)", c.KeyCipher, c.Key.Base64(), c.Msg)
}

const (
	BITS_128 = 128 / 8
	BITS_192 = 192 / 8
	BITS_256 = 256 / 8
)

func asymmetricEncryptKey(alg ASymCipher, raw crypto.PublicKey, key []byte) ([]byte, error) {
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

func initRandomSymmetricKey(rand io.Reader, alg SymCipher) ([]byte, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(CipherUnknownError, "Unknown cipher: %v", alg)
	case AES_128_GCM:
		return generateRandomBytes(rand, BITS_128)
	case AES_192_GCM:
		return generateRandomBytes(rand, BITS_192)
	case AES_256_GCM:
		return generateRandomBytes(rand, BITS_256)
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
func generateNonce(rand io.Reader, size int) ([]byte, error) {
	return generateRandomBytes(rand, size)
}

// Generates some random bytes (this should be considered )
func generateRandomBytes(rand io.Reader, size int) ([]byte, error) {
	arr := make([]byte, size)
	if _, err := io.ReadFull(rand, arr); err != nil {
		return nil, errors.WithStack(err)
	}
	return arr, nil
}
