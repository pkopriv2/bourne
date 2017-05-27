package warden

import (
	"io"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/stash"
)

type authProtocol string

// FIXME: Figure out versioning and compatibility strategy.
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
	Destroyer

	// Account lookup (usually a reference to an authenticator)
	Lookup() []byte

	// The protocol used to negotiate compatibility.
	Protocol() authProtocol

	// The arguments to be sent to the authenticator.  This value will be sent over
	// the wire, so it is HIGHLY advisable to make this extremely difficult to
	// reproduce.
	//
	// Caution:  Implementations MUST NEVER return a value that can derive the encryption
	// key of the corresponding member shard. Doing so would eliminate the zero-knowledge
	// guarantees of the protocol.
	Auth(io.Reader) ([]byte, error)

	// Derives an encryption seed may be used to decrypt the member's shard.
	Decrypt(memberShard) (Shard, error)

	// Derives a random shard
	Encrypt(io.Reader, Signer, Shard) (memberShard, error)
}

// Password based credentials
type passCred struct {
	User []byte
	Pass []byte // prehashed to avoid exposing consumer pass
}

func (p *passCred) Lookup() []byte {
	return stash.String("User://").Child(p.User)
}

func (p *passCred) Destroy() {
	cryptoBytes(p.Pass).Destroy()
	cryptoBytes(p.User).Destroy()
}

func (p *passCred) Protocol() authProtocol {
	return PassV1
}

func (p *passCred) Auth(rand io.Reader) ([]byte, error) {
	hash, err := hashN(p.Pass, SHA256, 2)
	return hash, errors.WithStack(err)
}

func (p *passCred) Seed([]byte) ([]byte, error) {
	hash, err := hashN(p.Pass, SHA256, 1)
	return hash, errors.WithStack(err)
}

func (p *passCred) Decrypt(shard memberShard) (Shard, error) {
	seed, err := p.Seed(shard.AuthArgs)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	raw, err := shard.Raw.Decrypt(seed)
	return raw, errors.WithStack(err)
}

func (p *passCred) Encrypt(rand io.Reader, signer Signer, shard Shard) (memberShard, error) {
	key, err := p.Seed(p.Pass)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	enc, err := encryptAndSignShard(rand, signer, shard, key)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	return memberShard{p.Lookup(), PassV1, enc, nil}, nil
}

// internal only (never stored)
type signCred struct {
	Signer Signer
	Hash   Hash
	Skew   time.Duration
}

func (p *signCred) Lookup() []byte {
	return stash.String("Signer://").ChildString(p.Signer.Public().Id())
}

func (p *signCred) Destroy() {
}

func (p *signCred) Protocol() authProtocol {
	return SignV1
}

func (p *signCred) Auth(rand io.Reader) ([]byte, error) {
	now := time.Now()

	fmt, err := gobBytes(now)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	sig, err := p.Signer.Sign(rand, p.Hash, fmt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, err := gobBytes(signArgs{p.Signer.Public(), now, sig, p.Skew})
	return ret, errors.WithStack(err)
}

func (p *signCred) Seed(nonce []byte) ([]byte, error) {
	sig, err := p.Signer.Sign(rand.New(rand.NewSource(0)), p.Hash, nonce)
	return sig.Data, errors.WithStack(err)
}

func (p *signCred) Decrypt(shard memberShard) (Shard, error) {
	seed, err := p.Seed(shard.AuthArgs)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	raw, err := shard.Raw.Decrypt(seed)
	return raw, errors.WithStack(err)
}

func (p *signCred) Encrypt(rand io.Reader, signer Signer, shard Shard) (memberShard, error) {
	nonce, err := genRandomBytes(rand, 32)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	key, err := p.Seed(nonce)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	enc, err := encryptAndSignShard(rand, signer, shard, key)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	return memberShard{p.Lookup(), SignV1, enc, nonce}, nil
}

// The password authenticator.
type passAuth struct {
	Ver  authProtocol
	Val  []byte
	Salt []byte
	Iter int
	Hash Hash
}

func newPassAuth(rand io.Reader, size, iter int, hash Hash, pass []byte) (passAuth, error) {
	salt, err := genRandomBytes(rand, size)
	if err != nil {
		return passAuth{}, errors.WithStack(err)
	}

	return passAuth{
		Ver:  PassV1,
		Val:  cryptoBytes(pass).Pbkdf2(salt, iter, size, hash.standard()),
		Salt: salt,
		Iter: iter,
		Hash: hash,
	}, nil
}

func (p *passAuth) Protocol() authProtocol {
	return p.Ver
}

func (p *passAuth) Authenticate(args []byte) error {
	derived := cryptoBytes(args).Pbkdf2(p.Salt, p.Iter, len(p.Val), p.Hash.standard())
	if !derived.Equals(p.Val) {
		return errors.Wrapf(UnauthorizedError, "Incorrect credentials")
	}
	return nil
}

// Signing authenticator arguments.
type signArgs struct {
	Pub  PublicKey // only used for creating the auth initially.
	Now  time.Time
	Sig  Signature
	Skew time.Duration
}

func parseSignArgs(raw []byte) (s signArgs, e error) {
	_, e = parseGobBytes(raw, &s)
	return
}

type signAuth struct {
	Ver authProtocol
	Pub PublicKey
}

func newSignAuth(raw []byte) (signAuth, error) {
	args, err := parseSignArgs(raw)
	if err != nil {
		return signAuth{}, errors.WithStack(err)
	}

	return signAuth{SignV1, args.Pub}, nil
}

func (s *signAuth) Protocol() authProtocol {
	return s.Ver
}

// Expects input to be: signAuthArgs
func (s *signAuth) Authenticate(raw []byte) error {
	args, err := parseSignArgs(raw)
	if err != nil {
		return errors.WithStack(err)
	}

	if args.Sig.Key != s.Pub.Id() {
		return errors.Wrapf(UnauthorizedError, "Unauthorized.  Signature does not match auth key")
	}

	if args.Skew > 10*time.Hour {
		return errors.Wrapf(UnauthorizedError, "Unauthorized.  Signature tolerance too high.")
	}

	if time.Now().Sub(args.Now) > args.Skew {
		return errors.Wrapf(UnauthorizedError, "Unauthorized.  Signature too old.")
	}

	msg, err := gobBytes(args.Now)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(args.Sig.Verify(s.Pub, msg))
}
