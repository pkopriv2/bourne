package krypto

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

var (
	ErrUnknownCipher = errors.New("Warden:UnknownCipher")
	ErrCipherKey     = errors.New("Warden:CipherKey")
)

// Supported symmetric ciphers.  This library is intended to ONLY offer support ciphers
// that implement the Authenticated Encryption with Associated Data (AEAD)
// standard.  Currently, that only includes the GCM family of streaming modes.
const (
	AES_128_GCM Cipher = iota
	AES_192_GCM
	AES_256_GCM
)

// Symmetric Cipher Type.  (FIXME: Switches are getting annoying...)
type Cipher int

func (s Cipher) KeySize() int {
	switch s {
	default:
		panic("UnknownCipher")
	case AES_128_GCM:
		return bits128
	case AES_192_GCM:
		return bits192
	case AES_256_GCM:
		return bits256
	}
}

func (s Cipher) String() string {
	switch s {
	default:
		return ErrUnknownCipher.Error()
	case AES_128_GCM:
		return "AES_128_GCM"
	case AES_192_GCM:
		return "AES_192_GCM"
	case AES_256_GCM:
		return "AES_256_GCM"
	}
}

func (s Cipher) Apply(rand io.Reader, key, msg []byte, enc ...string) (CipherText, error) {
	return Encrypt(rand, s, key, msg, enc...)
}

// Common bit->byte conversions
const (
	bits128 = 128 / 8
	bits192 = 192 / 8
	bits256 = 256 / 8
)

// TODO: Determine general set of fields for non-AE modes
//
// Currently thinking:
//  * Mac
type CipherText struct {
	Encoding string
	Cipher   Cipher
	Nonce    []byte
	Data     []byte
}

// Runs the given symmetric encryption algorithm on the message using the key as the key.  Returns the resulting cipher text
func Encrypt(rand io.Reader, alg Cipher, key, msg []byte, encoding ...string) (CipherText, error) {
	block, err := initBlockCipher(alg, key)
	if err != nil {
		return CipherText{}, errors.WithStack(err)
	}

	strm, err := initStreamCipher(alg, block)
	if err != nil {
		return CipherText{}, errors.WithStack(err)
	}

	nonce, err := GenNonce(rand, strm.NonceSize())
	if err != nil {
		return CipherText{}, errors.Wrapf(err, "Error generating nonce of [%v] bytes", strm.NonceSize())
	}

	var enc string = "unknown"
	if len(encoding) > 0 {
		enc = encoding[0]
	}

	return CipherText{enc, alg, nonce, strm.Seal(nil, nonce, msg, []byte(enc))}, nil
}

func (c CipherText) Decrypt(key []byte) (Bytes, error) {
	block, err := initBlockCipher(c.Cipher, key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	strm, err := initStreamCipher(c.Cipher, block)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, err := strm.Open(nil, c.Nonce, c.Data, []byte(c.Encoding))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return ret, nil
}

func (c CipherText) String() string {
	return fmt.Sprintf("CipherText(alg=%v,nonce=%v,data=%v)", c.Cipher, c.Nonce, Bytes(c.Data))
}

func initRandomSymmetricKey(rand io.Reader, alg Cipher) ([]byte, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(ErrUnknownCipher, "Unknown cipher: %v", alg)
	case AES_128_GCM:
		return genRandomBytes(rand, bits128)
	case AES_192_GCM:
		return genRandomBytes(rand, bits192)
	case AES_256_GCM:
		return genRandomBytes(rand, bits256)
	}
}

func initBlockCipher(alg Cipher, key []byte) (cipher.Block, error) {
	if err := ensureValidKey(alg, key); err != nil {
		return nil, errors.WithStack(err)
	}

	switch alg {
	default:
		return nil, errors.Wrapf(ErrUnknownCipher, "Unknown cipher: %v", alg)
	case AES_128_GCM, AES_192_GCM, AES_256_GCM:
		return aes.NewCipher(key)
	}
}

func initStreamCipher(alg Cipher, blk cipher.Block) (cipher.AEAD, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(ErrUnknownCipher, "Unknown cipher: %v", alg)
	case AES_128_GCM, AES_192_GCM, AES_256_GCM:
		return cipher.NewGCM(blk)
	}
}

// Creates a new random nonce.  Nonces are essentially the same
// thing as initialization vectors and should be use
func ensureValidKey(alg Cipher, key []byte) error {
	switch alg {
	default:
		return errors.Wrapf(ErrUnknownCipher, "Unknown cipher: %v", alg)
	case AES_128_GCM:
		return ensureKeySize(bits128, key)
	case AES_192_GCM:
		return ensureKeySize(bits192, key)
	case AES_256_GCM:
		return ensureKeySize(bits256, key)
	}
}

func ensureKeySize(expected int, key []byte) error {
	if expected != len(key) {
		return errors.Wrapf(ErrCipherKey, "Illegal key [%v].  Expected [%v] bytes but got [%v]", key, expected, len(key))
	} else {
		return nil
	}
}
