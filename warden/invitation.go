package warden

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type InvitationOptions struct {
	CertificateOptions
	ShareOpts OracleOptions
}

func buildInvitationOptions(opts ...func(*InvitationOptions)) InvitationOptions {
	def := InvitationOptions{buildCertificateOptions(), buildOracleOptions()}
	for _, fn := range opts {
		fn(&def)
	}
	return def
}

// An invitation is a cryptographically secured message asking the recipient to share in the
// management of a trust. The invitation may only be accepted by the intended recipient.
// These technically can be shared publicly, but exposure should be limited (typically only the
// trust system needs to know).
type Invitation struct {
	Id uuid.UUID

	// The embedded cert.
	Cert Certificate

	// The required signatures
	TrustSig  Signature
	IssuerSig Signature

	// The embedded curve information
	Key keyExchange
	Pt  securePoint
}

func (i Invitation) String() string {
	return fmt.Sprintf("Invitation(id=%v): %v", i.Id, i.Cert)
}

func generateInvitation(rand io.Reader,
	line line,
	cert Certificate,
	trustKey PrivateKey,
	issuerKey PrivateKey,
	trusteeKey PublicKey,
	opts InvitationOptions) (Invitation, error) {

	trustSig, err := cert.Sign(rand, trustKey, opts.ShareOpts.SigHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error signing certificate with key [%v]: %v", cert.Trust, cert)
	}

	issuerSig, err := cert.Sign(rand, issuerKey, opts.ShareOpts.SigHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error signing certificate with key [%v]: %v", cert.Trust, cert)
	}

	rawPt, err := generatePoint(rand, line, opts.ShareOpts.ShareCipher.KeySize())
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Unable to generate invitation for trustee [%v] to join [%v]", cert.Trustee, cert.Trust)
	}
	defer rawPt.Destroy()

	asymKey, cipherKey, err := generateKeyExchange(rand, trusteeKey, opts.ShareOpts.ShareCipher, opts.ShareOpts.ShareHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error generating key exchange [%v,%v]", Aes128Gcm, SHA256)
	}
	defer cryptoBytes(cipherKey).Destroy()

	encPt, err := encryptPoint(rand, rawPt, opts.ShareOpts.ShareCipher, cipherKey)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Unable to generate invitation for trustee [%v] to join [%v]", cert.Trustee, cert.Trust)
	}
	return Invitation{uuid.NewV1(), cert, trustSig, issuerSig, asymKey, encPt}, nil
}

// Verifies an invitation's signatures are valid.
func (i Invitation) verify(trustKey, issuerKey PublicKey) error {
	if err := i.Cert.Verify(trustKey, i.TrustSig); err != nil {
		return errors.WithStack(err)
	}
	if err := i.Cert.Verify(issuerKey, i.IssuerSig); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Accepts an invitation and returns an oracle key that can be registered.
func (i Invitation) accept(rand io.Reader,
	o Oracle,
	key PrivateKey,
	pass []byte,
	opts OracleOptions) (OracleKey, error) {

	pt, err := i.extractPoint(rand, key)
	if err != nil {
		return OracleKey{},
			errors.Wrapf(err, "Unable to extract curve point from invitation [%v]", i)
	}
	defer pt.Destroy()

	line, err := pt.Derive(o.Pt)
	if err != nil {
		return OracleKey{},
			errors.Wrapf(err, "Err obtaining deriving line for trust [%v]", i.Cert.Trust)
	}
	defer line.Destroy()

	oKey, err := genOracleKey(rand, line, pass, opts)
	if err != nil {
		return OracleKey{},
			errors.Wrapf(err, "Unable to generate new oracle key for trust [%v]", i.Cert.Trust)
	}
	return oKey, nil
}

// Verifies that the signature matches the certificate contents.
func (c Invitation) extractPoint(rand io.Reader, priv PrivateKey) (point, error) {
	cipherKey, err := c.Key.Decrypt(rand, priv)
	if err != nil {
		return point{}, errors.Wrapf(
			err, "Error extracting point from invitation [%v] using key [%v]", priv.Public().Id())
	}

	pt, err := c.Pt.Decrypt(cipherKey)
	if err != nil {
		return point{}, errors.Wrapf(
			err, "Error extracting point from invitation [%v] using key [%v]", priv.Public().Id())
	}
	return pt, nil
}
