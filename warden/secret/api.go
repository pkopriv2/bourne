package secret

import (
	"encoding"
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/warden/krypto"
)

var (
	ErrUnkownSecretAlgorithm = errors.New("Warden:UnknownSecretAlgorithm")
	ErrIncompatibleShard     = errors.New("Warden:IncompatibleShard")
)

type Secret interface {
	Opts() SecretOptions
	Hash(krypto.Hash) ([]byte, error)
	Shard(rand io.Reader) (Shard, error)
	Destroy()
}

// Generates a new random oracle + the curve that generated the oracle.  The returned curve
// may be used to generate oracle keys.
func genSecret(rand io.Reader, fns ...func(*SecretOptions)) (Secret, error) {
	opts := buildSecretOptions(fns...)
	secret, err := opts.Algorithm.Gen(rand, opts)
	return secret, errors.Wrap(err, "Error generating random secret")
}

type Shard interface {
	krypto.Signable

	// Implementations must eventually have platform independent encodings
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	Opts() SecretOptions
	Derive(Shard) (Secret, error) // TODO: Add support for threshold schemes
	Destroy()
}

// A signed oracle.  (Used to prove legitimacy of an oracle)
type SignedShard struct {
	Shard
	Sig krypto.Signature
}

// Signs the shard, returning a signed shard.
func SignShard(rand io.Reader, shard Shard, signer krypto.Signer, hash krypto.Hash) (SignedShard, error) {
	fmt, err := shard.SigningFormat()
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	sig, err := signer.Sign(rand, hash, fmt)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	return SignedShard{shard, sig}, nil
}

// Verifies the signed oracle
func (s SignedShard) Verify(key krypto.PublicKey) error {
	return errors.WithStack(krypto.Verify(s.Shard, key, s.Sig))
}

// Encrypts the shard.
func (s SignedShard) Encrypt(rand io.Reader, enc krypto.Encoder, cipher krypto.Cipher, pass krypto.Bytes) (EncryptedShard, error) {
	salt, err := krypto.GenSalt(rand)
	if err != nil {
		return EncryptedShard{}, errors.WithStack(err)
	}

	raw, err := enc.Encode(s)
	if err != nil {
		return EncryptedShard{}, errors.WithStack(err)
	}

	ct, err := salt.Encrypt(rand, cipher, pass, raw)
	if err != nil {
		return EncryptedShard{}, errors.WithStack(err)
	}

	return EncryptedShard(ct), nil
}

// An oracle key is required to unlock an oracle.  It contains a point on
// the corresponding oracle's secret line.
type EncryptedShard krypto.SaltedCipherText

func (p EncryptedShard) Decrypt(dec krypto.Decoder, pass []byte) (ret SignedShard, err error) {
	raw, err := krypto.SaltedCipherText(p).Decrypt(pass)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	err = dec.Decode(raw, &ret)
	return
}
