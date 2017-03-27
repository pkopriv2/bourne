package warden

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"io"
	"reflect"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

var (
	CipherUnknownError = errors.New("WARDEN:UNKNOWN_CIPHER")
	CipherKeyError     = errors.New("WARDEN:KEY_ERROR")
)

// Common bit->byte conversions
const (
	bits_128 = 128 / 8
	bits_192 = 192 / 8
	bits_256 = 256 / 8
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
type SymCipher int

func (s SymCipher) KeySize() int {
	switch s {
	default:
		panic("UnknownCipher")
	case AES_128_GCM:
		return bits_128
	case AES_192_GCM:
		return bits_192
	case AES_256_GCM:
		return bits_192
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
type AsymCipher int

const (
	RSA_WITH_SHA1 AsymCipher = iota
	RSA_WITH_SHA256
)

func (s AsymCipher) String() string {
	switch s {
	default:
		return "Unknown"
	case RSA_WITH_SHA1:
		return "RSA_WITH_SHA1"
	case RSA_WITH_SHA256:
		return "RSA_WITH_SHA256"
	}
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

// Runs the given symmetric encryption algorithm on the message using the key as the key.  Returns the resulting cipher text
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

func (c symCipherText) Decrypt(key []byte) (Bytes, error) {
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

func (c symCipherText) Bytes() []byte {
	return scribe.Write(c).Bytes()
}

func (c symCipherText) Write(w scribe.Writer) {
	w.WriteInt("cipher", int(c.Cipher))
	w.WriteBytes("nonce", c.Nonce)
	w.WriteBytes("data", c.Data)
}

func (c symCipherText) String() string {
	return fmt.Sprintf("SymCipherText(alg=%v,nonce=%v,data=%v)", c.Cipher, c.Nonce, c.Data)
}

func readSymCipherText(r scribe.Reader) (s symCipherText, err error) {
	err = r.ReadInt("cipher", (*int)(&s.Cipher))
	err = common.Or(err, r.ReadBytes("nonce", (*[]byte)(&s.Nonce)))
	err = common.Or(err, r.ReadBytes("data", (*[]byte)(&s.Data)))
	return
}

func parseSymCipherTextBytes(raw []byte) (symCipherText, error) {
	msg, err := scribe.Parse(raw)
	if err != nil {
		return symCipherText{}, err
	}
	return readSymCipherText(msg)
}

type asymCipherText struct {
	KeyCipher AsymCipher
	Key       Bytes
	Msg       symCipherText
}

func asymmetricEncrypt(rand io.Reader, keyCipher AsymCipher, msgCipher SymCipher, pub crypto.PublicKey, msg []byte) (asymCipherText, error) {
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
	key, err := asymmetricDecryptKey(c.KeyCipher, priv, c.Key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.Msg.Decrypt(key)
}

func (c asymCipherText) String() string {
	return fmt.Sprintf("AsymmetricCipherText(alg=%v,key=%v,val=%v)", c.KeyCipher, c.Key.Base64(), c.Msg)
}

func asymmetricEncryptKey(alg AsymCipher, raw crypto.PublicKey, key []byte) ([]byte, error) {
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

func asymmetricDecryptKey(alg AsymCipher, raw crypto.PrivateKey, key []byte) ([]byte, error) {
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
		return generateRandomBytes(rand, bits_128)
	case AES_192_GCM:
		return generateRandomBytes(rand, bits_192)
	case AES_256_GCM:
		return generateRandomBytes(rand, bits_256)
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
		return ensureKeySize(bits_128, key)
	case AES_192_GCM:
		return ensureKeySize(bits_192, key)
	case AES_256_GCM:
		return ensureKeySize(bits_256, key)
	}
}

func ensureKeySize(expected int, key []byte) error {
	if expected != len(key) {
		return errors.Wrapf(CipherKeyError, "Illegal key [%v].  Expected [%v] bytes but got [%v]", key, expected, len(key))
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
