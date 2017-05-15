package warden

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type InvitationOptions struct {
	Lvl LevelOfTrust
	Exp time.Duration
}

func buildInvitationOptions(opts ...func(*InvitationOptions)) InvitationOptions {
	def := InvitationOptions{Encrypt, 365 * 24 * time.Hour}
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

func createInvitation(rand io.Reader,
	secret Secret,
	cert Certificate,
	ringKey Signer,
	issuerKey Signer,
	trusteeKey PublicKey,
	opts TrustOptions) (Invitation, error) {

	shard, err := secret.Shard(rand)
	if err != nil {
		return Invitation{}, errors.Wrap(err, "Error creating shard from secret")
	}

	trustSig, err := sign(rand, cert, ringKey, opts.SignatureHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error signing certificate with key [%v]: %v", cert.Trust, cert)
	}

	issuerSig, err := sign(rand, cert, issuerKey, opts.SignatureHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error signing certificate with key [%v]: %v", cert.Trust, cert)
	}

	asymKey, cipherKey, err := generateKeyExchange(rand, trusteeKey, opts.InvitationCipher, opts.InvitationHash)
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

	ct, err := opts.InvitationCipher.encrypt(rand, cipherKey, msg)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Unable to generate invitation for trustee [%v] to join [%v]", cert.Trustee, cert.Trust)
	}

	return Invitation{uuid.NewV1(), cert, trustSig, issuerSig, asymKey, ct}, nil
}

// Accepts an invitation and returns an oracle key that can be registered.
func (i Invitation) acceptInvitation(cancel <-chan struct{}, s *Session) error {
	trust, ok, err := s.net.Trusts.ById(cancel, s.auth, i.Cert.Trust)
	if err != nil || !ok {
		return errors.WithStack(err)
	}

	mySecret, err := s.mySecret()
	if err != nil {
		return errors.WithStack(err)
	}
	defer mySecret.Destroy()

	mySecretKey, err := mySecret.Hash(SHA256)
	if err != nil {
		return errors.WithStack(err)
	}

	mySigningKey, err := s.mySigningKey(mySecret)
	if err != nil {
		return errors.WithStack(err)
	}

	myInviteKey, err := s.myInviteKey()
	if err != nil {
		return errors.WithStack(err)
	}

	myShard, err := i.accept(s.rand, mySigningKey, myInviteKey, trust.pubShard, mySecretKey)
	if err != nil {
		return errors.WithStack(err)
	}

	mySig, err := sign(s.rand, i.Cert, mySigningKey, trust.Opts.SignatureHash)
	if err != nil {
		return errors.WithStack(err)
	}

	cert := SignedCertificate{i.Cert, i.TrustSig, i.IssuerSig, mySig}
	return errors.WithStack(s.net.Certs.Register(cancel, s.auth, cert, myShard))
}

// Verifies an invitation's signatures are valid.
func (i Invitation) verify(trustKey, issuerKey PublicKey) error {
	if err := verify(i.Cert, trustKey, i.TrustSig); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(verify(i.Cert, issuerKey, i.IssuerSig))
}

// Accepts an invitation and returns an oracle key that can be registered.
func (i Invitation) accept(rand io.Reader, signer Signer, key PrivateKey, pub Shard, pass []byte) (signedEncryptedShard, error) {
	tmp, err := i.extractShard(rand, key, pub.Opts().ShardAlg)
	if err != nil {
		return signedEncryptedShard{}, errors.Wrapf(err, "Unable to extract curve point from invitation [%v]", i)
	}
	defer tmp.Destroy()

	secret, err := pub.Derive(tmp)
	if err != nil {
		return signedEncryptedShard{}, errors.Wrapf(err, "Err obtaining deriving secret [%v]", i.Cert.Trust)
	}
	defer secret.Destroy()

	new, err := secret.Shard(rand)
	if err != nil {
		return signedEncryptedShard{}, errors.Wrapf(err, "Err obtaining deriving secret [%v]", i.Cert.Trust)
	}
	defer new.Destroy()

	enc, err := encryptShard(rand, signer, new, pass)
	if err != nil {
		return signedEncryptedShard{}, errors.Wrapf(err, "Unable to generate new oracle key for trust [%v]", i.Cert.Trust)
	}
	return enc, nil
}

// Verifies that the signature matches the certificate contents.
func (c Invitation) extractShard(rand io.Reader, priv PrivateKey, alg SecretAlgorithm) (Shard, error) {
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
