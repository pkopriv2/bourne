package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type InvitationOptions struct {
	Cipher SymmetricCipher
	Hash   Hash
	Lvl    LevelOfTrust
	Ttl    time.Duration
}

func buildInvitationOptions(opts ...func(*InvitationOptions)) InvitationOptions {
	def := InvitationOptions{AES_128_GCM, SHA256, Encryption, 365 * 24 * time.Hour}
	for _, fn := range opts {
		fn(&def)
	}
	return def
}

func acceptInvitation(cancel <-chan struct{}, s Session, i Invitation, domainSig Signature, trusteeSig Signature) (Certificate, error) {
	priv, err := s.mySigningKey()
	if err != nil {
		return Certificate{}, errors.Wrapf(err, "Unable to retrieve session key [%v]", s.MyId())
	}

	defer priv.Destroy()

	pt, err := i.ExtractPoint(s.rand, priv)
	if err != nil {
		return Certificate{}, errors.Wrapf(err, "Unable to extract curve point from invitation [%v]", i)
	}

	defer pt.Destroy()

	domain, ok, err := LoadDomain(s, i.cert.Domain)
	if err != nil {
		return Certificate{}, errors.Wrapf(err, "Err obtaining auth token [%v]", s.MyId())
	}

	if !ok {
		return Certificate{}, errors.Wrapf(DomainInvariantError, "No such domain [%v]", i.cert.Domain)
	}

	// auth, err := s.auth(cancel)
	// if err != nil {
	// return Certificate{}, errors.Wrapf(err, "Err obtaining auth token [%v]", s.MyId())
	// }

	line, err := pt.Derive(domain.oracle.pt)
	if err != nil {
		return Certificate{}, errors.Wrapf(err, "Err obtaining deriving line for domain [%v]", s.MyId(), i.cert.Domain)
	}

	defer line.Destroy()
	//
	// key, err := generateOracleKey(s.rand, i.Domain, i.Trustee, line, s.seed, domain.oracleKeyOpts)
	// if err != nil {
	// return Certificate{}, errors.Wrapf(err, "Unable to generate new oracle key for domain [%v]", i.Domain)
	// }

	return Certificate{}, nil
}

// An invitation is a cryptographically secured message asking the recipient to share in the
// management of a domain. The invitation may only be accepted by the intended recipient.
// These technically can be shared publicly, but exposure should be limited (typically only the
// trust system needs to know).
type Invitation struct {
	Id uuid.UUID

	// Contains the embedded cert (must be signed by all parties)
	cert Certificate

	// The certificate signatures.  A valid certificate requires 3 signatures (issuer, domain, trustee)
	domainSig Signature
	issuerSig Signature

	// The embedded curve information
	key KeyExchange
	pt  securePoint
}

func generateInvitation(rand io.Reader, d Domain, oracleLine line, domainKey PrivateKey, issuerKey PrivateKey, trusteeKey PublicKey, fns ...func(*InvitationOptions)) (Invitation, error) {
	opts := buildInvitationOptions(fns...)


	cert := newCertificate(d.Id, issuerKey.Public().Id(), trusteeKey.Id(), opts.Lvl, opts.Ttl)

	domainSig, err := cert.Sign(rand, domainKey, SHA256)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error signing certificate with key [%v]: %v", domainKey.Public().Id(), cert)
	}

	issuerSig, err := cert.Sign(rand, issuerKey, SHA256)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error signing certificate with key [%v]: %v", issuerKey.Public().Id(), cert)
	}

	pt, err := generatePoint(rand, oracleLine, d.oracleKeyOpts.KeySize)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to generate invitation for trustee [%v] to join [%v]", trusteeKey.Id(), d.Id)
	}
	defer pt.Destroy()

	// FIXME: WRITE EXCHANGE METHOD
	encPt, err := encryptPoint(rand, pt, opts.Cipher, nil)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to generate invitation for trustee [%v] to join [%v]", trusteeKey.Id(), d.Id)
	}

	return Invitation{uuid.NewV1(), cert, domainSig, issuerSig, KeyExchange{}, encPt}, nil
}

// Return the byte representation of the invitation.  This *MUST* be totally deterministic!
func (i Invitation) Bytes() []byte {
	return nil
}

// Verifies that the signature matches the certificate contents.
func (c Invitation) ExtractPoint(rand io.Reader, priv PrivateKey) (point, error) {
	cipherKey, err := c.key.Decrypt(rand, priv)
	if err != nil {
		return point{}, errors.Wrapf(err, "Error extracting point from invitation [%v] using key [%v]", priv.Public().Id())
	}

	pt, err := c.pt.Decrypt(cipherKey)
	if err != nil {
		return point{}, errors.Wrapf(err, "Error extracting point from invitation [%v] using key [%v]", priv.Public().Id())
	}
	return pt, nil
}

// Verifies that the signature matches the certificate contents.
func (c Invitation) Verify(key PublicKey, signature Signature) error {
	return nil
}

// Signs the certificate with the private key.
func (c Invitation) Sign(rand io.Reader, key PrivateKey, hash Hash) (Signature, error) {
	return Signature{}, nil
}
