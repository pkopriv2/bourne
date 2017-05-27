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
	def := InvitationOptions{Manager, 365 * 24 * time.Hour}
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
	MsgKey  KeyExchange
	MsgData CipherText
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

	trustSig, err := sign(rand, cert, ringKey, opts.SigningHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error signing certificate with key [%v]: %v", cert.TrustId, cert)
	}

	issuerSig, err := sign(rand, cert, issuerKey, opts.SigningHash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error signing certificate with key [%v]: %v", cert.TrustId, cert)
	}

	asymKey, cipherKey, err := generateKeyExchange(rand, trusteeKey, opts.InvitationKey.Cipher, opts.InvitationKey.Hash)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error generating key exchange [%v,%v]", Aes128Gcm, SHA256)
	}
	defer cryptoBytes(cipherKey).Destroy()

	msg, err := shard.SigningFormat()
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Error generating bytes for shards.")
	}

	ct, err := opts.InvitationKey.Cipher.encrypt(rand, cipherKey, msg)
	if err != nil {
		return Invitation{}, errors.Wrapf(
			err, "Unable to generate invitation for trustee [%v] to join [%v]", cert.TrusteeId, cert.TrustId)
	}

	return Invitation{uuid.NewV1(), cert, trustSig, issuerSig, asymKey, ct}, nil
}

// Accepts an invitation and returns an oracle key that can be registered.
func (i Invitation) accept(cancel <-chan struct{}, s *Session) error {
	token, err := s.token(cancel)
	if err != nil {
		return errors.WithStack(err)
	}

	if i.Cert.TrusteeId != s.MyId() {
		return errors.Wrapf(UnauthorizedError, "Invitation bound for other recipient: %v", i.Cert.TrusteeId)
	}

	trust, ok, err := s.net.TrustById(cancel, token, i.Cert.TrustId)
	if err != nil {
		return errors.WithStack(err)
	}

	if !ok {
		return errors.Wrapf(UnknownTrustError, "No such trust [%v]", i.Cert.TrustId)
	}

	s.logger.Debug("Successfully loaded trust: %v", trust.Id)

	mySecret, err := s.mySecret()
	if err != nil {
		return errors.WithStack(err)
	}
	defer mySecret.Destroy()

	myEncryptionSeed, err := s.myEncryptionSeed(mySecret)
	if err != nil {
		return errors.WithStack(err)
	}

	mySigningKey, err := s.mySigningKey(mySecret)
	if err != nil {
		return errors.WithStack(err)
	}

	myInviteKey, err := s.myInvitationKey(mySecret)
	if err != nil {
		return errors.WithStack(err)
	}

	myShard, err := i.generateMemberShard(s.rand, mySigningKey, myInviteKey, trust.trustShard, myEncryptionSeed)
	if err != nil {
		return errors.WithStack(err)
	}

	myCertSig, err := sign(s.rand, i.Cert, mySigningKey, trust.Opts.SigningHash)
	if err != nil {
		return errors.WithStack(err)
	}

	cert := SignedCertificate{i.Cert, i.TrustSig, i.IssuerSig, myCertSig}
	s.logger.Debug("Generated certificate: %v", cert)

	return errors.WithStack(s.net.CertRegister(cancel, token, cert, TrustCode{i.Cert.TrusteeId, i.Cert.TrustId, myShard}))
}

// Verifies an invitation's signatures are valid.
func (i Invitation) verify(trustKey, issuerKey PublicKey) error {
	if err := verify(i.Cert, trustKey, i.TrustSig); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(verify(i.Cert, issuerKey, i.IssuerSig))
}

// Accepts an invitation and returns an oracle key that can be registered.
func (i Invitation) generateMemberShard(rand io.Reader, signer Signer, key PrivateKey, pub Shard, pass []byte) (SignedEncryptedShard, error) {
	tmp, err := i.extractShard(rand, key, pub.Opts().ShardAlg)
	if err != nil {
		return SignedEncryptedShard{}, errors.Wrapf(err, "Unable to extract curve point from invitation [%v]", i)
	}
	defer tmp.Destroy()

	secret, err := pub.Derive(tmp)
	if err != nil {
		return SignedEncryptedShard{}, errors.Wrapf(err, "Err obtaining deriving secret [%v]", i.Cert.TrustId)
	}
	defer secret.Destroy()

	new, err := secret.Shard(rand)
	if err != nil {
		return SignedEncryptedShard{}, errors.Wrapf(err, "Err obtaining deriving secret [%v]", i.Cert.TrustId)
	}
	defer new.Destroy()

	enc, err := encryptAndSignShard(rand, signer, new, pass)
	if err != nil {
		return SignedEncryptedShard{}, errors.Wrapf(err, "Unable to generate new oracle key for trust [%v]", i.Cert.TrustId)
	}
	return enc, nil
}

// Verifies that the signature matches the certificate contents.
func (c Invitation) extractShard(rand io.Reader, priv PrivateKey, alg SecretAlgorithm) (Shard, error) {
	cipherKey, err := c.MsgKey.Decrypt(rand, priv)
	if err != nil {
		return nil, errors.Wrapf(err, "Error extracting cipher key from invitation: %v", c.Id)
	}

	raw, err := c.MsgData.Decrypt(cipherKey)
	if err != nil {
		return nil, errors.Wrapf(err, "Error decrypting message from invitation: %v", c.Id)
	}

	ret, err := alg.Parse(raw)
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing shards from invitation: %v", c.Id)
	}

	return ret, nil
}
