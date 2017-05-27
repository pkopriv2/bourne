package warden

import (
	"io"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

type authProtocol string

const (
	PassProtocol authProtocol = "Pass/0.0.1" // try to use semantic versioning.
	SignProtocol              = "Sign/0.0.1"
)

// The authenticator is the server component responsible for authenticating a user.
type authenticator interface {
	MemberId() uuid.UUID
	Protocol() authProtocol
	Authenticate([]byte) error
}

// A Credential manages the client-side components of the
// login protocol.  In order to initiate the login, the consumer
// must provide two pieces of knowledge: 1) an account lookup
// and 2) the knowledge to authenticate.
//
//  1. Perform an account lookup.
//  2. Derive the account shard encryption key.
//
// For security reasons, credentials are short-lived.
//
type credential interface {
	// Destroyer

	// Account lookup (usually a reference to an authenticator)
	Lookup() []byte

	// // Account lookup (usually a reference to an authenticator)
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
type passCreds struct {
	User []byte
	Pass []byte // prehashed to avoid exposing consumer pass
}

func (p *passCreds) Lookup() []byte {
	return stash.String("User://").Child(p.User)
}

func (p *passCreds) Protocol() authProtocol {
	return PassProtocol
}

func (p *passCreds) Auth(rand io.Reader) ([]byte, error) {
	hash, err := hashN(p.Pass, SHA256, 2)
	return hash, errors.WithStack(err)
}

func (p *passCreds) Seed([]byte) ([]byte, error) {
	hash, err := hashN(p.Pass, SHA256, 1)
	return hash, errors.WithStack(err)
}

func (p *passCreds) Decrypt(shard MemberShard) (Shard, error) {
	seed, err := p.Seed(shard.Args)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	raw, err := shard.Raw.Decrypt(seed)
	return raw, errors.WithStack(err)
}

func (p *passCreds) Encrypt(rand io.Reader, signer Signer, shard Shard) (MemberShard, error) {
	key, err := p.Seed(p.Pass)
	if err != nil {
		return MemberShard{}, errors.WithStack(err)
	}

	enc, err := encryptAndSignShard(rand, signer, shard, key)
	if err != nil {
		return MemberShard{}, errors.WithStack(err)
	}

	return MemberShard{p.Lookup(), enc, nil, PassProtocol}, nil
}

// internal only (never stored)
type signCreds struct {
	Signer Signer
	Hash   Hash
}

func (p *signCreds) Lookup() []byte {
	return stash.String("Signer://").ChildString(p.Signer.Public().Id())
}

func (p *signCreds) Protocol() authProtocol {
	return SignProtocol
}

func (p *signCreds) Auth(rand io.Reader) ([]byte, error) {
	now := time.Now()

	fmt, err := gobBytes(now)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	sig, err := p.Signer.Sign(rand, p.Hash, fmt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, err := gobBytes(signAuthArgs{now, sig})
	return ret, errors.WithStack(err)
}

func (p *signCreds) Seed(nonce []byte) ([]byte, error) {
	sig, err := p.Signer.Sign(rand.New(rand.NewSource(0)), p.Hash, nonce)
	return sig.Data, errors.WithStack(err)
}

func (p *signCreds) Decrypt(shard MemberShard) (Shard, error) {
	seed, err := p.Seed(shard.Args)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	raw, err := shard.Raw.Decrypt(seed)
	return raw, errors.WithStack(err)
}

func (p *signCreds) Encrypt(rand io.Reader, signer Signer, shard Shard) (MemberShard, error) {
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

	return MemberShard{p.Lookup(), enc, nonce, PassProtocol}, nil
}

// The password authenticator.  A password authenticator only exists
// in a valid
type passAuth struct {
	Comp []byte
	Salt []byte
	Iter int
	Hash Hash
}

func (p passAuth) Authenticate(args []byte) bool {
	derived := cryptoBytes(args).Pbkdf2(p.Salt, p.Iter, len(p.Comp), p.Hash.standard())
	return derived.Equals(p.Comp)
}

type signAuthArgs struct {
	Now time.Time
	Sig Signature
}

type signAuth struct {
	Pub PublicKey
}

// Expects input to be: signAuthArgs
func (s *signAuth) Authenticate(raw []byte) error {
	var args signAuthArgs
	ok, err := parseGobBytes(raw, &args)
	if err != nil {
		return errors.WithStack(err)
	}
	if !ok {
		return errors.Wrap(AuthError, "No args provided.")
	}

	msg, err := gobBytes(args.Now)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(args.Sig.Verify(s.Pub, msg))
}
