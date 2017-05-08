package warden

import (
	"io"

	"github.com/pkg/errors"
)

type ShardingAlgorithm int

const (
	Shamir ShardingAlgorithm = iota
)

func (s ShardingAlgorithm) Parse(raw []byte) (ret Shard, err error) {
	switch s {
	default:
		return nil, errors.Wrapf(TrustError, "Unknown sharding algorithm [%v]", s)
	case Shamir:
		return parseShamirShard(raw)
	}
}

func (s ShardingAlgorithm) RandomSecret(rand io.Reader, strength int) (Secret, error) {
	switch s {
	default:
		return nil, errors.Wrapf(TrustError, "Unknown sharding algorithm [%v]", s)
	case Shamir:
		return generateShamirSecret(rand, strength)
	}
}

type Secret interface {
	Alg() ShardingAlgorithm
	Shard(rand io.Reader) (Shard, error)
	Format() ([]byte, error)
	Destroy()
}

type Shard interface {
	Alg() ShardingAlgorithm
	Derive(Shard) (Secret, error)
	Format() ([]byte, error)
	Destroy()
}

// Options for generating a shared secret.
type SecretOptions struct {

	// sharing algorithm
	ShardAlg      ShardingAlgorithm
	ShardStrength int

	// invitation options
	InviteCipher SymmetricCipher
	InviteHash   Hash
	InviteIter   int // used for key derivations only

	// signature options
	SigHash Hash
}

func defaultSecretOptions() SecretOptions {
	return SecretOptions{Shamir, 32, Aes256Gcm, SHA256, 1024, SHA256}
}

func buildSecretOptions(fns ...func(*SecretOptions)) SecretOptions {
	ret := defaultSecretOptions()
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A signed oracle.  (Used to prove legitimacy of an oracle)
type signedPublicShard struct {
	publicShard
	Sig Signature
}

// Verifies the signed oracle
func (s signedPublicShard) Verify(key PublicKey) error {
	fmt, err := s.Format()
	if err != nil {
		return err
	}

	return s.Sig.Verify(key, fmt)
}

// A signed oracle key.  (Used to prove legitimacy of raw key)
type signedPrivateShard struct {
	privateShard
	Sig Signature
}

// Verifies the signed oracle key
func (s signedPrivateShard) Verify(key PublicKey) error {
	fmt, err := s.Format()
	if err != nil {
		return err
	}

	return s.Sig.Verify(key, fmt)
}

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
type publicShard struct {

	// The public shard.
	Pub Shard

	// key options.  In order to share in the oracle, must agree to these
	Opts SecretOptions
}

// Generates a new random oracle + the curve that generated the oracle.  The returned curve
// may be used to generate oracle keys.
func genSharedSecret(rand io.Reader, opts SecretOptions) (publicShard, Secret, error) {
	secret, err := opts.ShardAlg.RandomSecret(rand, opts.ShardStrength)
	if err != nil {
		return publicShard{}, nil, errors.Wrap(err, "Error generating random secret")
	}

	shard, err := secret.Shard(rand)
	if err != nil {
		return publicShard{}, nil, errors.Wrap(err, "Error generating shards")
	}

	return publicShard{shard, opts}, secret, nil
}

// Decrypts the oracle, returning a hash of the underlying secret.
func (p publicShard) Derive(other Shard) (Secret, error) {
	return p.Pub.Derive(other)
}

func (p publicShard) Sign(rand io.Reader, priv Signer, hash Hash) (signedPublicShard, error) {
	fmt, err := p.Format()
	if err != nil {
		return signedPublicShard{}, err
	}

	sig, err := priv.Sign(rand, hash, fmt)
	if err != nil {
		return signedPublicShard{}, err
	}

	return signedPublicShard{p, sig}, nil
}

func (p publicShard) Format() ([]byte, error) {
	return gobBytes(p)
}

// An oracle key is required to unlock an oracle.  It contains a point on
// the corresponding oracle's secret line.
type privateShard struct {
	Alg ShardingAlgorithm
	Msg cipherText

	// encryption key derivation arguments point
	KeyHash Hash
	KeySalt []byte
	KeyIter int
}

// Generates a new key for the oracle whose secret was derived from line.  Only the given pass
// may unlock the oracle key.
func genPrivateShard(rand io.Reader, secret Secret, pass []byte, opts SecretOptions) (privateShard, error) {
	size := opts.InviteCipher.KeySize()

	shard, err := secret.Shard(rand)
	if err != nil {
		return privateShard{}, errors.Wrap(err, "Error generating generating secret shard")
	}

	fmt, err := shard.Format()
	if err != nil {
		return privateShard{}, errors.WithStack(err)
	}

	salt, err := generateRandomBytes(rand, size)
	if err != nil {
		return privateShard{}, errors.Wrapf(err, "Error generating salt of strength [%v]", size)
	}

	key := cryptoBytes(pass).Pbkdf2(salt, opts.InviteIter, size, opts.InviteHash.standard())

	ct, err := symmetricEncrypt(rand, opts.InviteCipher, key, fmt)
	if err != nil {
		return privateShard{}, errors.WithStack(err)
	}

	return privateShard{secret.Alg(), ct, opts.InviteHash, salt, opts.InviteIter}, nil
}

func (p privateShard) Sign(rand io.Reader, priv Signer, hash Hash) (signedPrivateShard, error) {
	fmt, err := p.Format()
	if err != nil {
		return signedPrivateShard{}, err
	}

	sig, err := priv.Sign(rand, hash, fmt)
	if err != nil {
		return signedPrivateShard{}, err
	}

	return signedPrivateShard{p, sig}, nil
}

func (p privateShard) Format() ([]byte, error) {
	return gobBytes(p)
}

func (p privateShard) Decrypt(pass []byte) (Shard, error) {
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
