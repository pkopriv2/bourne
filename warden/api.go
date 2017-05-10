package warden

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
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
	SessionExpiredError = errors.New("Warden:ExpiredSession")
	SessionInvalidError = errors.New("Warden:InvalidSession")
	InvariantError      = errors.New("Warden:InvariantError")
	TrustError          = errors.New("Warden:TrustError")
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

// Registers a new subscription with the trust service.
func Subscribe(ctx common.Context, addr string, creds func(KeyPad)) (Session, error) {
	return Session{}, nil
}

// Loads a subscription
func Connect(ctx common.Context, addr string, creds func(KeyPad)) (Session, error) {
	return Session{}, nil
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

// A key pad gives access to the various authentication methods and will
// be used during the registration process.
//
// Future: Accept alternative login methods (e.g. pins, passwords, multi-factor, etc...)
type KeyPad interface {

	// Authenticates using a simple signature scheme. The signer will be asked
	// to sign a simple message, which will then be used as proof that the caller
	// has direct signing access to the private key component of the public key.
	//
	// Note: The public key is only used as a means of performing a simple account
	// lookup and is not used to verify the signature.
	WithSignature(signer Signer) (Session, error)
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
