package warden

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

var (
	CipherUnknownError = errors.New("Warden:CipherUnknown")
	CipherKeyError     = errors.New("Warden:CipherKey")
)

// Common bit->byte conversions
const (
	bits128 = 128 / 8
	bits192 = 192 / 8
	bits256 = 256 / 8
)

// Supported symmetric ciphers.  This library is intended to ONLY offer support ciphers
// that implement the Authenticated Encryption with Associated Data (AEAD)
// standard.  Currently, that only includes the GCM family of streaming modes.
const (
	Aes128Gcm SymmetricCipher = iota
	Aes192Gcm
	Aes256Gcm
)

// Symmetric Cipher Type.  (FIXME: Switches are getting annoying...)
type SymmetricCipher int

func (s SymmetricCipher) KeySize() int {
	switch s {
	default:
		panic("UnknownCipher")
	case Aes128Gcm:
		return bits128
	case Aes192Gcm:
		return bits192
	case Aes256Gcm:
		return bits256
	}
}

func (s SymmetricCipher) String() string {
	switch s {
	default:
		return CipherUnknownError.Error()
	case Aes128Gcm:
		return "AES_128_GCM"
	case Aes192Gcm:
		return "AES_192_GCM"
	case Aes256Gcm:
		return "AES_256_GCM"
	}
}

func (s SymmetricCipher) encrypt(rand io.Reader, key []byte, msg []byte) (cipherText, error) {
	return symmetricEncrypt(rand, s, key, msg)
}

// TODO: Determine general set of fields for non-AE modes
//
// Currently thinking:
//  * Mac
type cipherText struct {
	Cipher SymmetricCipher
	Nonce  cryptoBytes
	Data   cryptoBytes
}

// Runs the given symmetric encryption algorithm on the message using the key as the key.  Returns the resulting cipher text
func symmetricEncrypt(rand io.Reader, alg SymmetricCipher, key []byte, msg []byte) (cipherText, error) {
	block, err := initBlockCipher(alg, key)
	if err != nil {
		return cipherText{}, errors.WithStack(err)
	}

	strm, err := initStreamCipher(alg, block)
	if err != nil {
		return cipherText{}, errors.WithStack(err)
	}

	nonce, err := generateNonce(rand, strm.NonceSize())
	if err != nil {
		return cipherText{}, errors.Wrapf(err, "Error generating nonce of [%v] bytes", strm.NonceSize())
	}

	return cipherText{alg, nonce, strm.Seal(nil, nonce, msg, nil)}, nil
}

func (c cipherText) Decrypt(key []byte) (cryptoBytes, error) {
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

func (c cipherText) Bytes() []byte {
	return scribe.Write(c).Bytes()
}

func (c cipherText) Write(w scribe.Writer) {
	w.WriteInt("cipher", int(c.Cipher))
	w.WriteBytes("nonce", c.Nonce)
	w.WriteBytes("data", c.Data)
}

func (c cipherText) String() string {
	return fmt.Sprintf("SymCipherText(alg=%v,nonce=%v,data=%v)", c.Cipher, c.Nonce, c.Data)
}

func readCipherText(r scribe.Reader) (s cipherText, err error) {
	err = r.ReadInt("cipher", (*int)(&s.Cipher))
	err = common.Or(err, r.ReadBytes("nonce", (*[]byte)(&s.Nonce)))
	err = common.Or(err, r.ReadBytes("data", (*[]byte)(&s.Data)))
	return
}

func parseCipherTextBytes(raw []byte) (cipherText, error) {
	msg, err := scribe.Parse(raw)
	if err != nil {
		return cipherText{}, err
	}
	return readCipherText(msg)
}

type keyExchange struct {
	KeyAlg    KeyAlgorithm
	KeyCipher SymmetricCipher
	KeyHash   Hash
	KeyBytes  cryptoBytes
}

func generateKeyExchange(rand io.Reader, pub PublicKey, cipher SymmetricCipher, hash Hash) (keyExchange, []byte, error) {
	rawCipherKey, err := initRandomSymmetricKey(rand, cipher)
	if err != nil {
		return keyExchange{}, nil, errors.WithStack(err)
	}

	encCipherKey, err := pub.Encrypt(rand, hash, rawCipherKey)
	if err != nil {
		return keyExchange{}, nil, errors.WithStack(err)
	}

	return keyExchange{pub.Algorithm(), cipher, hash, encCipherKey}, rawCipherKey, nil
}

func (k keyExchange) Decrypt(rand io.Reader, priv PrivateKey) ([]byte, error) {
	key, err := priv.Decrypt(rand, k.KeyHash, k.KeyBytes)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return key, nil
}

func (c keyExchange) String() string {
	return fmt.Sprintf("AsymmetricCipherText(alg=%v,key=%v,val=%v)", c.KeyAlg, c.KeyHash, c.KeyBytes)
}

func initRandomSymmetricKey(rand io.Reader, alg SymmetricCipher) ([]byte, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(CipherUnknownError, "Unknown cipher: %v", alg)
	case Aes128Gcm:
		return generateRandomBytes(rand, bits128)
	case Aes192Gcm:
		return generateRandomBytes(rand, bits192)
	case Aes256Gcm:
		return generateRandomBytes(rand, bits256)
	}
}

func initBlockCipher(alg SymmetricCipher, key []byte) (cipher.Block, error) {
	if err := ensureValidKey(alg, key); err != nil {
		return nil, errors.WithStack(err)
	}

	switch alg {
	default:
		return nil, errors.Wrapf(CipherUnknownError, "Unknown cipher: %v", alg)
	case Aes128Gcm, Aes192Gcm, Aes256Gcm:
		return aes.NewCipher(key)
	}
}

func initStreamCipher(alg SymmetricCipher, blk cipher.Block) (cipher.AEAD, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(CipherUnknownError, "Unknown cipher: %v", alg)
	case Aes128Gcm, Aes192Gcm, Aes256Gcm:
		return cipher.NewGCM(blk)
	}
}

// Creates a new random nonce.  Nonces are essentially the same
// thing as initialization vectors and should be use
func ensureValidKey(alg SymmetricCipher, key []byte) error {
	switch alg {
	default:
		return errors.Wrapf(CipherUnknownError, "Unknown cipher: %v", alg)
	case Aes128Gcm:
		return ensureKeySize(bits128, key)
	case Aes192Gcm:
		return ensureKeySize(bits192, key)
	case Aes256Gcm:
		return ensureKeySize(bits256, key)
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
