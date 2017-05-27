package warden

import (
	"io"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/stash"
)

type authProtocol string

const (
	PassV1 authProtocol = "Pass/0.0.1" // try to use semantic versioning.
	SignV1              = "Sign/0.0.1"
)

// The authenticator is the server component responsible for authenticating a user.
type authenticator interface {
	Protocol() authProtocol
	Authenticate([]byte) error
}

// A credential manages the client-side components of the login protocol.  In order
// to initiate the login, the consumer must provide two pieces of knowledge:
//
//   * an account lookup
//   * an authentication value
//
//
// For security reasons, credentials are short-lived.
//
type credential interface {

	// Account lookup (usually a reference to an authenticator)
	Lookup() []byte

	// Account lookup (usually a reference to an authenticator)
	Protocol() authProtocol

	// The arguments to be sent to the authenticator.  This value will
	// be sent over the wire, so it is HIGHLY advisable to make this
	// extremely difficult to reproduce.
	//
	// Caution:  Implementations MUST NEVER return a value that can
	// derive the encryption key of the corresponding member shard.
	Auth(io.Reader) ([]byte, error)

	// Derives an encryption seed may be used to decrypt the member's shard.
	Decrypt(MemberShard) (Shard, error)

	// Derives a random shard
	Encrypt(io.Reader, Signer, Shard) (MemberShard, error)
}

// Password based credentials
type passV1Creds struct {
	User []byte
	Pass []byte // prehashed to avoid exposing consumer pass
}

func (p *passV1Creds) Lookup() []byte {
	return stash.String("User://").Child(p.User)
}

func (p *passV1Creds) Protocol() authProtocol {
	return PassV1
}

func (p *passV1Creds) Auth(rand io.Reader) ([]byte, error) {
	hash, err := hashN(p.Pass, SHA256, 2)
	return hash, errors.WithStack(err)
}

func (p *passV1Creds) Seed([]byte) ([]byte, error) {
	hash, err := hashN(p.Pass, SHA256, 1)
	return hash, errors.WithStack(err)
}

func (p *passV1Creds) Decrypt(shard MemberShard) (Shard, error) {
	seed, err := p.Seed(shard.Args)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	raw, err := shard.Raw.Decrypt(seed)
	return raw, errors.WithStack(err)
}

func (p *passV1Creds) Encrypt(rand io.Reader, signer Signer, shard Shard) (MemberShard, error) {
	key, err := p.Seed(p.Pass)
	if err != nil {
		return MemberShard{}, errors.WithStack(err)
	}

	enc, err := encryptAndSignShard(rand, signer, shard, key)
	if err != nil {
		return MemberShard{}, errors.WithStack(err)
	}

	return MemberShard{p.Lookup(), PassV1, enc, nil}, nil
}

// internal only (never stored)
type signV1Creds struct {
	Signer Signer
	Hash   Hash
}

func (p *signV1Creds) Lookup() []byte {
	return stash.String("Signer://").ChildString(p.Signer.Public().Id())
}

func (p *signV1Creds) Protocol() authProtocol {
	return SignV1
}

func (p *signV1Creds) Auth(rand io.Reader) ([]byte, error) {
	now := time.Now()

	fmt, err := gobBytes(now)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	sig, err := p.Signer.Sign(rand, p.Hash, fmt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, err := gobBytes(signV1AuthArgs{p.Signer.Public(), now, sig})
	return ret, errors.WithStack(err)
}

func (p *signV1Creds) Seed(nonce []byte) ([]byte, error) {
	sig, err := p.Signer.Sign(rand.New(rand.NewSource(0)), p.Hash, nonce)
	return sig.Data, errors.WithStack(err)
}

func (p *signV1Creds) Decrypt(shard MemberShard) (Shard, error) {
	seed, err := p.Seed(shard.Args)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	raw, err := shard.Raw.Decrypt(seed)
	return raw, errors.WithStack(err)
}

func (p *signV1Creds) Encrypt(rand io.Reader, signer Signer, shard Shard) (MemberShard, error) {
	nonce, err := genRandomBytes(rand, 32)
	if err != nil {
		return MemberShard{}, errors.WithStack(err)
	}

	key, err := p.Seed(nonce)
	if err != nil {
		return MemberShard{}, errors.WithStack(err)
	}

	enc, err := encryptAndSignShard(rand, signer, shard, key)
	if err != nil {
		return MemberShard{}, errors.WithStack(err)
	}

	return MemberShard{p.Lookup(), SignV1, enc, nonce}, nil
}

// The password authenticator.  A password authenticator only exists
// in a valid
type passV1Auth struct {
	Ver  authProtocol
	Val  []byte
	Salt []byte
	Iter int
	Hash Hash
}

func newV1PassAuth(rand io.Reader, size, iter int, hash Hash, pass []byte) (passV1Auth, error) {
	salt, err := genRandomBytes(rand, size)
	if err != nil {
		return passV1Auth{}, errors.WithStack(err)
	}

	return passV1Auth{
		Ver:  PassV1,
		Val:  cryptoBytes(pass).Pbkdf2(salt, iter, size, hash.standard()),
		Salt: salt,
		Iter: iter,
		Hash: hash,
	}, nil
}

func (p *passV1Auth) Protocol() authProtocol {
	return p.Ver
}

func (p *passV1Auth) Authenticate(args []byte) error {
	derived := cryptoBytes(args).Pbkdf2(p.Salt, p.Iter, len(p.Val), p.Hash.standard())
	if !derived.Equals(p.Val) {
		return errors.Wrapf(AuthError, "Bad Password")
	}
	return nil
}

type signV1AuthArgs struct {
	Pub PublicKey // only used for creating the auth initially.
	Now time.Time
	Sig Signature
}

func parseSignAuthArgs(raw []byte) (s signV1AuthArgs, e error) {
	_, e = parseGobBytes(raw, &s)
	return
}


type signV1Auth struct {
	Ver authProtocol
	Pub PublicKey
}

func newSignAuth(raw []byte) (signV1Auth, error) {
	args, err := parseSignAuthArgs(raw)
	if err != nil {
		return signV1Auth{}, errors.WithStack(err)
	}

	return signV1Auth{SignV1, args.Pub}, nil
}

func (s *signV1Auth) Protocol() authProtocol {
	return s.Ver
}

// Expects input to be: signAuthArgs
func (s *signV1Auth) Authenticate(raw []byte) error {
	args, err := parseSignAuthArgs(raw)
	if err != nil {
		return errors.WithStack(err)
	}

	msg, err := gobBytes(args.Now)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(args.Sig.Verify(s.Pub, msg))
}
