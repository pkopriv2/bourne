package warden

import (
	"crypto/rand"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/micro"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// FIXMES:
//	* Get off gob encoding/decoding so we can easily support other languages.

// Useful references:
//
// * https://www.owasp.org/index.php/Key_Management_Cheat_Sheet
// * Secret sharing survey:
//		* https://www.cs.bgu.ac.il/~beimel/Papers/Survey.pdf
// * Computational secrecy:
//		* http://www.cs.cornell.edu/courses/cs754/2001fa/secretshort.pdf
// * SSRI (Secure store retrieval and )
//		* http://ac.els-cdn.com/S0304397598002631/1-s2.0-S0304397598002631-main.pdf?_tid=d4544ec8-221e-11e7-9744-00000aacb35f&acdnat=1492290326_2f9f40490893fb853da9d080f5b47634
// * Blakley's scheme
//		* https://en.wikipedia.org/wiki/Secret_sharing#Blakley.27s_scheme

// Common errors
var (
	RpcError           = errors.New("Warden:RpcError")
	TokenExpiredError  = errors.New("Warden:TokenExpired")
	TokenInvalidError  = errors.New("Warden:TokenInvalid")
	UnauthorizedError  = errors.New("Warden:Unauthorized")
	InvariantError     = errors.New("Warden:InvariantError")
	TrustError         = errors.New("Warden:TrustError")
	UnknownMemberError = errors.New("Warden:UnknownMember")
	UnknownTrustError  = errors.New("Warden:UnknownTrust")
)

// The paging options
type PagingOptions struct {
	Beg int
	End int
}

// Constructs paging options.
func buildPagingOptions(fns ...func(p *PagingOptions)) PagingOptions {
	opts := PagingOptions{0, 256}
	for _, fn := range fns {
		fn(&opts)
	}
	return opts
}

// Subscribe options
type SubscribeOptions struct {
	SessionOptions

	SecretAlgorithm SecretAlgorithm
	SecretStrength  int

	SigningKeyAlg      KeyAlgorithm
	SigningKeyStrength int

	InviteKeyAlg      KeyAlgorithm
	InviteKeyStrength int

	Deps *Dependencies
}

func (s *SubscribeOptions) memberOptions() func(o *MemberOptions) {
	return func(o *MemberOptions) {
		o.SecretOptions(func(o *SecretOptions) {
			o.ShardAlg = s.SecretAlgorithm
			o.ShardStrength = s.SecretStrength
		})

		o.SigningOptions(func(o *KeyPairOptions) {
			o.Algorithm = s.SigningKeyAlg
			o.Strength = s.SigningKeyStrength
		})

		o.InviteOptions(func(o *KeyPairOptions) {
			o.Algorithm = s.InviteKeyAlg
			o.Strength = s.InviteKeyStrength
		})
	}
}

func buildSubscribeOptions(fns ...func(*SubscribeOptions)) SubscribeOptions {
	ret := SubscribeOptions{
		SessionOptions:     buildSessionOptions(),
		SecretAlgorithm:    Shamir,
		SecretStrength:     32,
		SigningKeyAlg:      Rsa,
		SigningKeyStrength: 2048,
		InviteKeyAlg:       Rsa,
		InviteKeyStrength:  2048,
	}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// Connection options.
type ConnectOptions struct {
	SessionOptions
	Deps *Dependencies
}

func buildConnectOptions(fns ...func(*ConnectOptions)) ConnectOptions {
	ret := ConnectOptions{SessionOptions: buildSessionOptions()}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// Core dependencies
type Dependencies struct {
	Net  Transport
	Rand io.Reader
}

func buildDependencies(ctx common.Context, addr string, timeout time.Duration, fns ...func(*Dependencies)) (Dependencies, error) {
	ret := Dependencies{}
	for _, fn := range fns {
		fn(&ret)
	}

	if ret.Rand == nil {
		ret.Rand = rand.Reader
	}

	if ret.Net == nil {
		conn, err := net.NewTcpNetwork().Dial(timeout, addr)
		if err != nil {
			return Dependencies{}, errors.WithStack(err)
		}

		cl, err := micro.NewClient(ctx, conn, micro.Gob)
		if err != nil {
			return Dependencies{}, errors.WithStack(err)
		}
		ret.Net = newClient(cl)
	}

	return ret, nil
}

// Registers a new subscription with the trust service.
func Subscribe(ctx common.Context, addr string, login func(KeyPad) error, fns ...func(*SubscribeOptions)) (*Session, error) {
	creds, err := enterCreds(login)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	opts := buildSubscribeOptions(fns...)
	if opts.Deps == nil {
		deps, err := buildDependencies(ctx, addr, opts.SessionOptions.NetTimeout)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		opts.Deps = &deps
	}

	timer := ctx.Timer(opts.NetTimeout)
	defer timer.Closed()

	member, code, err := newMember(opts.Deps.Rand, creds, opts.memberOptions())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	token, err := opts.Deps.Net.Register(timer.Closed(), member, code, opts.SessionOptions.TokenExpiration)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	session, err := newSession(ctx, member, code, token, login, opts.SessionOptions, *opts.Deps)
	return session, errors.WithStack(err)
}

// Connects
func Connect(ctx common.Context, addr string, login func(KeyPad) error, fns ...func(*ConnectOptions)) (*Session, error) {
	creds, err := enterCreds(login)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	opts := buildConnectOptions(fns...)
	if opts.Deps == nil {
		deps, err := buildDependencies(ctx, addr, opts.SessionOptions.NetTimeout)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		opts.Deps = &deps
	}

	timer := ctx.Timer(opts.SessionOptions.NetTimeout)
	defer timer.Closed()

	token, err := auth(timer.Closed(), opts.Deps.Rand, opts.Deps.Net, creds, opts.SessionOptions)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	mem, ac, o, err := opts.Deps.Net.MemberByLookup(timer.Closed(), token, newSigLookup(creds.Signer.Public()))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !o {
		return nil, errors.Wrapf(UnauthorizedError, "No such member.")
	}

	session, err := newSession(ctx, mem, ac, token, login, opts.SessionOptions, *opts.Deps)
	return session, errors.WithStack(err)
}

func authBySignature(cancel <-chan struct{}, rand io.Reader, net Transport, signer Signer, opts SessionOptions) (Token, error) {
	ch, sig, err := newSigChallenge(rand, signer, opts.TokenHash)
	if err != nil {
		return Token{}, errors.WithStack(err)
	}

	token, err := net.TokenBySignature(cancel, newSigLookup(signer.Public()), ch, sig, opts.TokenExpiration)
	return token, errors.WithStack(err)
}

func auth(cancel <-chan struct{}, rand io.Reader, net Transport, creds *oneTimePad, opts SessionOptions) (Token, error) {
	if creds.Signer != nil {
		return authBySignature(cancel, rand, net, creds.Signer, opts)
	}
	return Token{}, errors.Wrap(InvariantError, "Invalid login type")
}

// A signer contains the knowledge necessary to digitally sign messages.
//
// For high-security environments, only a signer is required to authenticate with the
// system.  It is never necessary for the corresponding private key to leave the local
// system, which is convienent in enviroments where access to the private key is not
// possible (e.g. embedded hardware security modules).
type Signer interface {
	Public() PublicKey
	Sign(rand io.Reader, hash Hash, msg []byte) (Signature, error)
}

// A signature is a cryptographically secure structure that may be used to both prove
// the authenticity of an accompanying document, as well as the identity of the signer.
type Signature struct {
	Key  string
	Hash Hash
	Data []byte
}

// Verifies the signature with the given public key.  Returns nil if the verification succeeded.
func (s Signature) Verify(key PublicKey, msg []byte) error {
	return key.Verify(s.Hash, msg, s.Data)
}

// Returns a simple string rep of the signature.
func (s Signature) String() string {
	return fmt.Sprintf("Signature(hash=%v): %v", s.Hash, cryptoBytes(s.Data))
}

// A formatter just formats a particular object.  Used to produce consistent digital signatures.
type Formatter interface {
	Format() ([]byte, error)
}

// Signs the object with the signer and hash
func sign(rand io.Reader, obj Formatter, signer Signer, hash Hash) (Signature, error) {
	fmt, err := obj.Format()
	if err != nil {
		return Signature{}, errors.WithStack(err)
	}

	sig, err := signer.Sign(rand, hash, fmt)
	if err != nil {
		return Signature{}, errors.WithStack(err)
	}
	return sig, nil
}

// Verifies the object with the signer and hash
func verify(obj Formatter, key PublicKey, sig Signature) error {
	fmt, err := obj.Format()
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(key.Verify(sig.Hash, fmt, sig.Data))
}

// Decrypts the object with the key.
func decrypt(obj Decrypter, key Formatter) ([]byte, error) {
	fmt, err := key.Format()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ret, err := obj.Decrypt(fmt)
	return ret, errors.WithStack(err)
}

// A decrypter decrypts itself with the given key
type Decrypter interface {
	Decrypt(key []byte) ([]byte, error)
}

// A destroyer simply destroys itself.
type Destroyer interface {
	Destroy()
}

// Public keys are the basis of identity within the trust ecosystem.  In plain english,
// I don't trust your identity, I only trust your keys.  Therefore, risk planning starts
// with limiting the exposure of your trusted keys.  The more trusted a key, the greater
// the implications of a compromised one.
//
// The purpose of using Warden is not just to publish keys for your own personal use
// (although it may be used for that), it is ultimately expected to serve as a way that
// people can form groups of trust.
//
type PublicKey interface {
	Id() string
	Algorithm() KeyAlgorithm
	Verify(hash Hash, msg []byte, sig []byte) error
	Encrypt(rand io.Reader, hash Hash, plaintext []byte) ([]byte, error)
	format() []byte
}

// Private keys represent proof of ownership of a public key and form the basis of
// how authentication, confidentiality and integrity concerns are managed within the
// ecosystem.  Warden goes to great lengths to ensure that private keys data are *NEVER*
// derivable by any untrusted actors (malicious or otherwise), including the system itself.
//
type PrivateKey interface {
	Signer
	Algorithm() KeyAlgorithm
	Decrypt(rand io.Reader, hash Hash, ciphertext []byte) ([]byte, error)
	format() []byte
	Destroy()
}

type Document struct {
	Id       uuid.UUID
	Name     string
	Ver      int
	Created  time.Time
	Contents []byte
}

func (d Document) Bytes() error {
	return nil
}

func (d Document) Sign(key PrivateKey, hash Hash) (Signature, error) {
	return Signature{}, nil
}
