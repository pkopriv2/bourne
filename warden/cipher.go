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
	bits_128 = 128 / 8
	bits_192 = 192 / 8
	bits_256 = 256 / 8
)

// Supported symmetric ciphers.  This library is intended to ONLY offer support ciphers
// that implement the Authenticated Encryption with Associated Data (AEAD)
// standard.  Currently, that only includes the GCM family of streaming modes.
const (
	AES_128_GCM SymmetricCipher = iota
	AES_192_GCM
	AES_256_GCM
)

// Symmetric Cipher Type.  (FIXME: Switches are getting annoying...)
type SymmetricCipher int

func (s SymmetricCipher) KeySize() int {
	switch s {
	default:
		panic("UnknownCipher")
	case AES_128_GCM:
		return bits_128
	case AES_192_GCM:
		return bits_192
	case AES_256_GCM:
		return bits_256
	}
}

func (s SymmetricCipher) String() string {
	switch s {
	default:
		return CipherUnknownError.Error()
	case AES_128_GCM:
		return "AES_128_GCM"
	case AES_192_GCM:
		return "AES_192_GCM"
	case AES_256_GCM:
		return "AES_256_GCM"
	}
}

// TODO: Determine general set of fields for non-AE modes
//
// Currently thinking:
//  * Mac
type CipherText struct {
	Cipher SymmetricCipher
	Nonce  Bytes
	Data   Bytes
}

// Runs the given symmetric encryption algorithm on the message using the key as the key.  Returns the resulting cipher text
func symmetricEncrypt(rand io.Reader, alg SymmetricCipher, key []byte, msg []byte) (CipherText, error) {
	block, err := initBlockCipher(alg, key)
	if err != nil {
		return CipherText{}, errors.WithStack(err)
	}

	strm, err := initStreamCipher(alg, block)
	if err != nil {
		return CipherText{}, errors.WithStack(err)
	}

	nonce, err := generateNonce(rand, strm.NonceSize())
	if err != nil {
		return CipherText{}, errors.Wrapf(err, "Error generating nonce of [%v] bytes", strm.NonceSize())
	}

	return CipherText{alg, nonce, strm.Seal(nil, nonce, msg, nil)}, nil
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

	ret, err := strm.Open(nil, c.Nonce, c.Data, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return ret, nil
}

func (c CipherText) Bytes() []byte {
	return scribe.Write(c).Bytes()
}

func (c CipherText) Write(w scribe.Writer) {
	w.WriteInt("cipher", int(c.Cipher))
	w.WriteBytes("nonce", c.Nonce)
	w.WriteBytes("data", c.Data)
}

func (c CipherText) String() string {
	return fmt.Sprintf("SymCipherText(alg=%v,nonce=%v,data=%v)", c.Cipher, c.Nonce, c.Data)
}

func readCipherText(r scribe.Reader) (s CipherText, err error) {
	err = r.ReadInt("cipher", (*int)(&s.Cipher))
	err = common.Or(err, r.ReadBytes("nonce", (*[]byte)(&s.Nonce)))
	err = common.Or(err, r.ReadBytes("data", (*[]byte)(&s.Data)))
	return
}

func parseCipherTextBytes(raw []byte) (CipherText, error) {
	msg, err := scribe.Parse(raw)
	if err != nil {
		return CipherText{}, err
	}
	return readCipherText(msg)
}

type KeyExchange struct {
	KeyAlg    KeyAlgorithm
	KeyCipher SymmetricCipher
	KeyHash   Hash
	CipherKey Bytes
}

func generateKeyExchange(rand io.Reader, pub PublicKey, cipher SymmetricCipher, hash Hash) (KeyExchange, []byte, error) {
	rawCipherKey, err := initRandomSymmetricKey(rand, cipher)
	if err != nil {
		return KeyExchange{}, nil, errors.WithStack(err)
	}

	encCipherKey, err := pub.Encrypt(rand, hash, rawCipherKey)
	if err != nil {
		return KeyExchange{}, nil, errors.WithStack(err)
	}

	return KeyExchange{pub.Algorithm(), cipher, hash, encCipherKey}, rawCipherKey, nil
}

func (k KeyExchange) Decrypt(rand io.Reader, priv PrivateKey) ([]byte, error) {
	key, err := priv.Decrypt(rand, k.KeyHash, k.CipherKey)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return key, nil
}

func (c KeyExchange) String() string {
	return fmt.Sprintf("AsymmetricCipherText(alg=%v+%v,key=%v,val=%v)", c.KeyAlg, c.KeyHash, c.CipherKey.Base64())
}

func initRandomSymmetricKey(rand io.Reader, alg SymmetricCipher) ([]byte, error) {
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

func initBlockCipher(alg SymmetricCipher, key []byte) (cipher.Block, error) {
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

func initStreamCipher(alg SymmetricCipher, blk cipher.Block) (cipher.AEAD, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(CipherUnknownError, "Unknown cipher: %v", alg)
	case AES_128_GCM, AES_192_GCM, AES_256_GCM:
		return cipher.NewGCM(blk)
	}
}

// Creates a new random nonce.  Nonces are essentially the same
// thing as initialization vectors and should be use
func ensureValidKey(alg SymmetricCipher, key []byte) error {
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
