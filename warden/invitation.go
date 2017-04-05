package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type InvitationOptions struct {
	Lvl        LevelOfTrust
	Expiration time.Duration
	ShareOpts  oracleOptions
}

func buildInvitationOptions(opts ...func(*InvitationOptions)) InvitationOptions {
	def := InvitationOptions{Encryption, 365 * 24 * time.Hour, buildOracleOptions()}
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

	key, err := generateOracleKey(s.rand, i.Cert.Domain, i.Cert.Trustee, line, s.oracle, domain.oracle.opts)
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

	// The embedded curve information
	key KeyExchange
	pt  securePoint
}

func generateInvitation(rand io.Reader, line line, domainKey PrivateKey, issuerKey PrivateKey, trusteeKey PublicKey, fns ...func(*InvitationOptions)) (Invitation, error) {
	opts := buildInvitationOptions(fns...)

	cert := newCertificate(
		domainKey.Public().Id(),
		issuerKey.Public().Id(),
		trusteeKey.Id(),
		opts.Lvl,
		opts.Expiration)

	domainSig, err := cert.Sign(rand, domainKey, opts.ShareOpts.SigHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error signing certificate with key [%v]: %v", cert.Domain, cert)
	}

	issuerSig, err := cert.Sign(rand, issuerKey, opts.ShareOpts.SigHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error signing certificate with key [%v]: %v", cert.Domain, cert)
	}

	rawPt, err := generatePoint(rand, line, opts.ShareOpts.ShareCipher.KeySize())
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Unable to generate invitation for trustee [%v] to join [%v]", cert.Trustee, cert.Domain)
	}
	defer rawPt.Destroy()

	asymKey, cipherKey, err := generateKeyExchange(rand, trusteeKey, opts.ShareOpts.ShareCipher, opts.ShareOpts.ShareHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error generating key exchange [%v,%v]", AES_128_GCM, SHA256)
	}
	defer Bytes(cipherKey).Destroy()

	encPt, err := encryptPoint(rand, rawPt, opts.ShareOpts.ShareCipher, cipherKey)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Unable to generate invitation for trustee [%v] to join [%v]", cert.Trustee, cert.Domain)
	}
	return Invitation{uuid.NewV1(), cert, domainSig, issuerSig, asymKey, encPt}, nil
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
