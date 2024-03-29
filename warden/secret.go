package warden

import (
	"io"

	"github.com/pkg/errors"
)

// An oracle is used to protect a secret that may be shared amongst many actors.
//
// An oracle protects the secret by creating a curve (in this case a line) and
// publishing a point on the curve.  Only entities which can provide the other
// points required to rederive the curve can unlock an oracle.   To provide an
// an "unlocking" mechanism, there is also an oracle key which contains a
// point on the line that has been encrypted with a symmetric cipher key.  The
// key can be anything, from a password to another key or even another oracle's
// seed value.
//
// The purpose of the oracle is to act as an encryption seeding algorithm.
// The seed must be paired with other knowledge to be useful, meaning the risk
// of a leaked shared oracle is minimal.  Even though this is a critical
// component, it can't act alone.
//
type SecretAlgorithm int

const (
	ShamirAlpha SecretAlgorithm = iota
)

func (s SecretAlgorithm) Parse(raw []byte) (ret Shard, err error) {
	switch s {
	default:
		return nil, errors.Wrapf(UnauthorizedError, "Unknown sharding algorithm [%v]", s)
	case ShamirAlpha:
		return parseShamirShard(raw)
	}
}

func (s SecretAlgorithm) RandomSecret(rand io.Reader, opts secretOptions) (Secret, error) {
	switch s {
	default:
		return nil, errors.Wrapf(UnauthorizedError, "Unknown sharding algorithm [%v]", s)
	case ShamirAlpha:
		return generateShamirSecret(rand, opts)
	}
}

// Options for generating a shared secret.
type secretOptions struct {
	ShardAlg      SecretAlgorithm
	ShardStrength int

	SecretHash Hash

	// shard encryption options
	ShardCipher SymmetricCipher
	ShardHash   Hash
	ShardIter   int // used for key derivations only
	ShardSalt   int
}

func defaultSecretOptions() secretOptions {
	return secretOptions{ShamirAlpha, 32, SHA256, Aes256Gcm, SHA256, 1024, 32}
}

func buildSecretOptions(fns ...func(*secretOptions)) secretOptions {
	ret := defaultSecretOptions()
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

type Secret interface {
	Hash(Hash) ([]byte, error)
	Opts() secretOptions
	Shard(rand io.Reader) (Shard, error)
	Destroy()
}

type Shard interface {
	Signable

	Opts() secretOptions
	Derive(Shard) (Secret, error)
	Destroy()
}

// Generates a new random oracle + the curve that generated the oracle.  The returned curve
// may be used to generate oracle keys.
func genSecret(rand io.Reader, opts secretOptions) (Secret, error) {
	secret, err := opts.ShardAlg.RandomSecret(rand, opts)
	return secret, errors.Wrap(err, "Error generating random secret")
}

// Signs the shard, returning a signed shard.
func signShard(rand io.Reader, signer Signer, shard Shard) (SignedShard, error) {
	opts := shard.Opts()

	fmt, err := shard.SigningFormat()
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	sig, err := signer.Sign(rand, opts.ShardHash, fmt)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	return SignedShard{shard, sig}, nil
}

// Encrypts and signs the shard.
func encryptShard(rand io.Reader, shard Shard, pass []byte) (EncryptedShard, error) {
	opts := shard.Opts()

	fmt, err := shard.SigningFormat()
	if err != nil {
		return EncryptedShard{}, errors.WithStack(err)
	}

	salt, err := genRandomBytes(rand, opts.ShardSalt)
	if err != nil {
		return EncryptedShard{}, errors.Wrapf(err, "Error generating salt of strength [%v]")
	}

	key := cryptoBytes(pass).Pbkdf2(salt, opts.ShardIter, opts.ShardCipher.KeySize(), opts.ShardHash.standard())

	ct, err := symmetricEncrypt(rand, opts.ShardCipher, key, fmt)
	if err != nil {
		return EncryptedShard{}, errors.WithStack(err)
	}

	return EncryptedShard{opts.ShardAlg, ct, opts.ShardHash, salt, opts.ShardIter}, nil
}

// Encrypts and signs the shard.
func encryptAndSignShard(rand io.Reader, signer Signer, shard Shard, pass []byte) (SignedEncryptedShard, error) {
	opts := shard.Opts()

	enc, err := encryptShard(rand, shard, pass)
	if err != nil {
		return SignedEncryptedShard{}, errors.WithStack(err)
	}

	ret, err := enc.Sign(rand, signer, opts.ShardHash)
	return ret, errors.WithStack(err)
}

// A signed oracle.  (Used to prove legitimacy of an oracle)
type SignedShard struct {
	Shard
	Sig Signature
}

// Verifies the signed oracle
func (s SignedShard) Verify(key PublicKey) error {
	return errors.WithStack(verify(s.Shard, key, s.Sig))
}

// A signed oracle key.  (Used to prove legitimacy of raw key)
type SignedEncryptedShard struct {
	EncryptedShard
	Sig Signature
}

// Verifies the signed oracle key
func (s SignedEncryptedShard) Verify(key PublicKey) error {
	fmt, err := s.Format()
	if err != nil {
		return err
	}
	return s.Sig.Verify(key, fmt)
}

// An oracle key is required to unlock an oracle.  It contains a point on
// the corresponding oracle's secret line.
type EncryptedShard struct {
	Alg SecretAlgorithm
	Msg CipherText

	// encryption key derivation arguments point
	KeyHash Hash
	KeySalt []byte
	KeyIter int
}

func (p EncryptedShard) Sign(rand io.Reader, priv Signer, hash Hash) (SignedEncryptedShard, error) {
	fmt, err := p.Format()
	if err != nil {
		return SignedEncryptedShard{}, err
	}

	sig, err := priv.Sign(rand, hash, fmt)
	if err != nil {
		return SignedEncryptedShard{}, err
	}

	return SignedEncryptedShard{p, sig}, nil
}

func (p EncryptedShard) Format() ([]byte, error) {
	return gobBytes(p)
}

func (p EncryptedShard) Decrypt(pass []byte) (Shard, error) {
	key := cryptoBytes(pass).Pbkdf2(p.KeySalt, p.KeyIter, p.Msg.Cipher.KeySize(), p.KeyHash.standard())

	raw, err := p.Msg.Decrypt(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, err := p.Alg.Parse(raw)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return ret, nil
}
