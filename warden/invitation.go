package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type InvitationOptions struct {
	ExchgCipher SymmetricCipher
	Hash        Hash
	Lvl         LevelOfTrust
	Ttl         time.Duration
	SigHash     Hash
}

func buildInvitationOptions(opts ...func(*InvitationOptions)) InvitationOptions {
	def := InvitationOptions{AES_128_GCM, SHA256, Encryption, 365 * 24 * time.Hour, SHA256}
	for _, fn := range opts {
		fn(&def)
	}
	return def
}

func acceptInvitation(cancel <-chan struct{}, s Session, i Invitation) error {
	priv, err := s.mySigningKey()
	if err != nil {
		return errors.Wrapf(err, "Unable to retrieve session key [%v]", s.MyId())
	}
	defer priv.Destroy()

	sig, err := i.Cert.Sign(s.rand, priv, SHA256)
	if err != nil {
		return errors.Wrapf(err, "Error signing certificate: ", i.Cert)
	}

	pt, err := i.extractPoint(s.rand, priv)
	if err != nil {
		return errors.Wrapf(err, "Unable to extract curve point from invitation [%v]", i)
	}
	defer pt.Destroy()

	domain, ok, err := LoadDomain(s, i.Cert.Domain)
	if err != nil {
		return errors.Wrapf(err, "Err obtaining auth token [%v]", s.MyId())
	}

	if !ok {
		return errors.Wrapf(DomainInvariantError, "No such domain [%v]", i.Cert.Domain)
	}

	auth, err := s.auth(cancel)
	if err != nil {
		return errors.Wrapf(err, "Err obtaining auth token [%v]", s.MyId())
	}

	line, err := pt.Derive(domain.oracle.pt)
	if err != nil {
		return errors.Wrapf(err, "Err obtaining deriving line for domain [%v]", s.MyId(), i.Cert.Domain)
	}
	defer line.Destroy()

	key, err := generateOracleKey(s.rand, i.Cert.Domain, i.Cert.Trustee, line, s.seed, domain.oracleKeyOpts)
	if err != nil {
		return errors.Wrapf(err, "Unable to generate new oracle key for domain [%v]", i.Cert.Domain)
	}

	err = s.net.RegisterCertificate(cancel, auth, i.Cert, i.DomainSig, i.IssuerSig, sig, key)
	return errors.Wrapf(err, "Unable to register certificate [%v]", i.Cert)
}

// An invitation is a cryptographically secured message asking the recipient to share in the
// management of a domain. The invitation may only be accepted by the intended recipient.
// These technically can be shared publicly, but exposure should be limited (typically only the
// trust system needs to know).
type Invitation struct {
	Id uuid.UUID

	// Contains the embedded cert (must be signed by all parties)
	Cert Certificate

	// The domain signature.  A valid certificate requires 3 signatures (issuer, domain, trustee)
	DomainSig Signature

	// The issuer signature.
	IssuerSig Signature

	// The embedded curve information (internal only)
	key KeyExchange
	pt  securePoint
}

func generateInvitation(rand io.Reader, d Domain, oracleLine line, domainKey PrivateKey, issuerKey PrivateKey, trusteeKey PublicKey, fns ...func(*InvitationOptions)) (Invitation, error) {
	opts := buildInvitationOptions(fns...)

	cert := newCertificate(d.Id, issuerKey.Public().Id(), trusteeKey.Id(), opts.Lvl, opts.Ttl)

	domainSig, err := cert.Sign(rand, domainKey, opts.SigHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error signing certificate with key [%v]: %v", domainKey.Public().Id(), cert)
	}

	issuerSig, err := cert.Sign(rand, issuerKey, opts.SigHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error signing certificate with key [%v]: %v", issuerKey.Public().Id(), cert)
	}

	pt, err := generatePoint(rand, oracleLine, d.oracleKeyOpts.Cipher.KeySize())
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to generate invitation for trustee [%v] to join [%v]", trusteeKey.Id(), d.Id)
	}
	defer pt.Destroy()

	exchg, cipherKey, err := generateKeyExchange(rand, trusteeKey, opts.ExchgCipher, opts.Hash)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error generating key exchange [%v,%v]", AES_128_GCM, SHA256)
	}
	defer Bytes(cipherKey).Destroy()

	encPt, err := encryptPoint(rand, pt, opts.ExchgCipher, cipherKey)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to generate invitation for trustee [%v] to join [%v]", trusteeKey.Id(), d.Id)
	}
	return Invitation{uuid.NewV1(), cert, domainSig, issuerSig, exchg, encPt}, nil
}

// Verifies that the signature matches the certificate contents.
func (c Invitation) extractPoint(rand io.Reader, priv PrivateKey) (point, error) {
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
