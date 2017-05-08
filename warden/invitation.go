package warden

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type InvitationOptions struct {
	CertificateOptions
	ShareOpts SecretOptions
}

func buildInvitationOptions(opts ...func(*InvitationOptions)) InvitationOptions {
	def := InvitationOptions{buildCertificateOptions(), buildSecretOptions()}
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

	Cert      Certificate
	TrustSig  Signature
	IssuerSig Signature

	// The embedded secret information
	key keyExchange
	msg cipherText
}

func (i Invitation) String() string {
	return fmt.Sprintf("Invitation(id=%v): %v", i.Id, i.Cert)
}

func generateInvitation(rand io.Reader,
	secret Secret,
	cert Certificate,
	ringKey Signer,
	issuerKey Signer,
	trusteeKey PublicKey,
	opts InvitationOptions) (Invitation, error) {

	shard, err := secret.Shard(rand)
	if err != nil {
		return Invitation{}, errors.Wrap(err, "Error creating shard from secret")
	}

	trustSig, err := cert.Sign(rand, ringKey, opts.ShareOpts.SigHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error signing certificate with key [%v]: %v", cert.Trust, cert)
	}

	issuerSig, err := cert.Sign(rand, issuerKey, opts.ShareOpts.SigHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error signing certificate with key [%v]: %v", cert.Trust, cert)
	}

	asymKey, cipherKey, err := generateKeyExchange(rand, trusteeKey, opts.ShareOpts.InviteCipher, opts.ShareOpts.InviteHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error generating key exchange [%v,%v]", Aes128Gcm, SHA256)
	}
	defer cryptoBytes(cipherKey).Destroy()

	msg, err := shard.Format()
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error generating bytes for shards.")
	}

	ct, err := opts.ShareOpts.InviteCipher.encrypt(rand, cipherKey, msg)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Unable to generate invitation for trustee [%v] to join [%v]", cert.Trustee, cert.Trust)
	}

	return Invitation{uuid.NewV1(), cert, trustSig, issuerSig, asymKey, ct}, nil
}

// Accepts an invitation and returns an oracle key that can be registered.
func (i Invitation) acceptInvitation(cancel <-chan struct{}, s *Session) error {
	dom, ok, err := s.net.Trusts.ById(cancel, s.auth, i.Cert.Trust)
	if err != nil || !ok {
		return errors.WithStack(err)
	}

	priv, err := s.mySigningKey()
	if err != nil {
		return errors.WithStack(err)
	}

	key, err := i.accept(s.rand, dom.oracle.publicShard, priv, s.myOracle(), dom.oracle.Opts)
	if err != nil {
		return errors.WithStack(err)
	}

	sig, err := i.Cert.Sign(s.rand, priv, dom.oracle.Opts.SigHash)
	if err != nil {
		return errors.WithStack(err)
	}

	return s.net.Certs.Register(cancel, s.auth, i.Cert, key, i.TrustSig, i.IssuerSig, sig)
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
	o publicShard, key PrivateKey,
	pass []byte,
	opts SecretOptions) (privateShard, error) {

	shard, err := i.extractShard(rand, key, o.Pub.Alg())
	if err != nil {
		return privateShard{},
			errors.Wrapf(err, "Unable to extract curve point from invitation [%v]", i)
	}
	defer shard.Destroy()

	secret, err := o.Pub.Derive(shard)
	if err != nil {
		return privateShard{},
			errors.Wrapf(err, "Err obtaining deriving secret [%v]", i.Cert.Trust)
	}
	defer secret.Destroy()

	oKey, err := genPrivateShard(rand, secret, pass, opts)
	if err != nil {
		return privateShard{},
			errors.Wrapf(err, "Unable to generate new oracle key for trust [%v]", i.Cert.Trust)
	}
	return oKey, nil
}

// Verifies that the signature matches the certificate contents.
func (c Invitation) extractShard(rand io.Reader, priv PrivateKey, alg ShardingAlgorithm) (Shard, error) {
	cipherKey, err := c.key.Decrypt(rand, priv)
	if err != nil {
		return nil, errors.Wrapf(err, "Error extracting cipher key from invitation: %v", c.Id)
	}

	raw, err := c.msg.Decrypt(cipherKey)
	if err != nil {
		return nil, errors.Wrapf(err, "Error decrypting message from invitation: %v", c.Id)
	}

	ret, err := alg.Parse(raw)
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing shards from invitation: %v", c.Id)
	}

	return ret, nil
}
