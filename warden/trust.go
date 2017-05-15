package warden

import (
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const OneHundredYears = 100 * 365 * 24 * time.Hour

type KeyType int

const (
	SigningKey KeyType = iota
	InviteKey
)

// General trust options.
type TrustOptions struct {
	Secret SecretOptions

	// ecryption key options
	EncryptionKey CipherKeyOptions

	// Invitation options
	InvitationKey CipherKeyOptions

	// signature options
	SigningHash Hash

	// default key options.
	SigningKey KeyPairOptions
}

func buildTrustOptions(fns ...func(*TrustOptions)) TrustOptions {
	ret := TrustOptions{buildSecretOptions(), buildCipherKeyOpts(), buildCipherKeyOpts(), SHA256, buildKeyPairOptions()}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A trust is a personal view of a digital trust.  Fields will be populated
// based on the subscriber's relationship with the trust.
type Trust struct {
	Id uuid.UUID

	// the common name of the trust. (not advertized, but public)
	Name string

	// the options of the trust
	Opts TrustOptions

	// the signing key pair.
	signingKey SignedKeyPair

	// the public portion of the shared secret
	pubShard SignedShard

	// the certificate affirming the caller's relationship with the trust
	myCert SignedCertificate

	// the private shard portion of the shared secret. (may only be unlocked by owner)
	myShard SignedEncryptedShard
}

// generates a trust, but has no server-side effects.
func newTrust(s *Session, name string, fns ...func(s *TrustOptions)) (Trust, error) {
	opts := buildTrustOptions(fns...)

	mySecret, err := s.mySecret()
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error extracting session signing key [%v]", s.MyId())
	}
	defer mySecret.Destroy()

	myEncryptionSeed, err := mySecret.Hash(opts.Secret.SecretHash)
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error extracting session signing key [%v]", s.MyId())
	}
	defer mySecret.Destroy()

	mySigningKey, err := s.mySigningKey(mySecret)
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error extracting session signing key [%v]", s.MyId())
	}
	defer mySigningKey.Destroy()

	trustSigningKey, err := opts.SigningKey.Algorithm.Gen(s.rand, opts.SigningKey.Strength)
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error generating  key [%v]: %v", opts.SigningKey.Algorithm, opts.SigningKey.Strength)
	}
	defer trustSigningKey.Destroy()

	secret, err := genSecret(s.rand, opts.Secret)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer secret.Destroy()

	pubShard, err := secret.Shard(s.rand)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer pubShard.Destroy()

	myShard, err := secret.Shard(s.rand)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer myShard.Destroy()

	pubShardSig, err := signShard(s.rand, mySigningKey, pubShard)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	myShardEnc, err := encryptShard(s.rand, mySigningKey, myShard, myEncryptionSeed)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	signingKey, err := encryptKey(s.rand, mySigningKey, trustSigningKey, myEncryptionSeed, opts.SigningKey)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	cert := newCertificate(uuid.NewV1(), s.MyId(), s.MyId(), Creator, OneHundredYears)

	signedCert, err := signCertificate(
		s.rand, cert, trustSigningKey, mySigningKey, mySigningKey, opts.SigningHash)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	return Trust{
		Id:         cert.Trust,
		Name:       name,
		signingKey: signingKey,
		myCert:     signedCert,
		Opts:       opts,
		pubShard:   pubShardSig,
		myShard:    myShardEnc,
	}, nil
}

// Extracts the  oracle curve.  Requires *Encrypt* level trust
func (d Trust) unlockSecret(s *Session) (Secret, error) {
	if err := Encrypt.verify(d.myCert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	mySecret, err := s.mySecret()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	mySecretKey, err := s.myEncryptionSeed(mySecret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	myShard, err := d.myShard.Decrypt(mySecretKey)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	secret, err := d.pubShard.Derive(myShard)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return secret, nil
}

func (d Trust) unlockEncryptionSeed(secret Secret) ([]byte, error) {
	key, err := secret.Hash(d.Opts.Secret.SecretHash)
	return key, errors.WithStack(err)
}

func (d Trust) unlockSigningKey(cancel <-chan struct{}, s *Session, secret Secret) (PrivateKey, error) {
	if err := Sign.verify(d.myCert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	key, err := d.unlockEncryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	priv, err := d.signingKey.Decrypt(key)
	return priv, errors.WithStack(err)
}

func (d Trust) renewCertificate(cancel <-chan struct{}, s *Session) (Trust, error) {
	secret, err := d.unlockSecret(s)
	if err != nil {
		return Trust{}, errors.Wrap(err, "Error deriving trust secret.")
	}
	defer secret.Destroy()

	trustSigningKey, err := d.unlockSigningKey(cancel, s, secret)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer trustSigningKey.Destroy()

	mySecret, err := s.mySecret()
	if err != nil {
		return Trust{}, errors.Wrapf(err, "Error extracting session signing key [%v]", s.MyId())
	}
	defer mySecret.Destroy()

	myEncryptionSeed, err := s.myEncryptionSeed(mySecret)
	if err != nil {
		return Trust{}, errors.Wrapf(err, "Error extracting session signing key [%v]", s.MyId())
	}
	defer cryptoBytes(myEncryptionSeed).Destroy()

	mySigningKey, err := s.mySigningKey(mySecret)
	if err != nil {
		return Trust{}, errors.Wrapf(err, "Error extracting session signing key [%v]", s.MyId())
	}
	defer mySigningKey.Destroy()

	cert := newCertificate(d.Id, s.MyId(), s.MyId(), d.myCert.Level, d.myCert.Duration())

	myCert, err := signCertificate(
		s.rand, cert, trustSigningKey, mySigningKey, mySigningKey, d.Opts.SigningHash)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	myShard, err := secret.Shard(s.rand)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer myShard.Destroy()

	myShardEnc, err := encryptShard(s.rand, mySigningKey, myShard, myEncryptionSeed)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	if err := s.net.CertRegister(cancel, s.auth, myCert, myShardEnc); err != nil {
		return Trust{}, errors.Wrapf(err, "Error renewing subscriber [%v] cert to trust [%v]", s.MyId(), d.Id)
	}

	return Trust{
		Id:         d.Id,
		Name:       d.Name,
		Opts:       d.Opts,
		signingKey: d.signingKey,
		pubShard:   d.pubShard,
		myShard:    myShardEnc,
	}, nil
}

// Loads all the trust certificates that have been issued by this .
func (t Trust) listCertificates(cancel <-chan struct{}, s Session, fns ...func(*PagingOptions)) ([]Certificate, error) {
	if err := Verify.verify(t.myCert.Level); err != nil {
		return nil, errors.WithStack(err)
	}
	return s.net.CertsByTrust(cancel, s.auth, t.Id, buildPagingOptions(fns...))
}

// Revokes all issued certificates by this  for the given subscriber.
func (t Trust) revokeCertificate(cancel <-chan struct{}, s *Session, trustee uuid.UUID) error {
	if err := Revoke.verify(t.myCert.Level); err != nil {
		return errors.WithStack(err)
	}

	if err := s.net.CertRevoke(cancel, s.auth, t.myCert.Id); err != nil {
		return errors.Wrapf(err, "Unable to revoke certificate [%v] for subscriber [%v]", t.myCert.Id, trustee)
	}

	return nil
}

// Issues an invitation to the given key.
func (t Trust) invite(cancel <-chan struct{}, s *Session, trusteeId uuid.UUID, trusteeKey PublicKey, fns ...func(*InvitationOptions)) (Invitation, error) {
	if err := Invite.verify(t.myCert.Level); err != nil {
		return Invitation{}, newLevelOfTrustError(Invite, t.myCert.Level)
	}

	opts := buildInvitationOptions(fns...)

	mySecret, err := s.mySecret()
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to unlock  oracle [%v]", t.Id)
	}
	defer mySecret.Destroy()

	secret, err := t.unlockSecret(s)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to unlock  oracle [%v]", t.Id)
	}
	defer secret.Destroy()

	ringKey, err := t.unlockSigningKey(cancel, s, secret)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Erro retrieving signing key [%v]", t.Id)
	}
	defer ringKey.Destroy()

	issuerKey, err := s.mySigningKey(mySecret)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving my signing key [%v]", s.MyId())
	}
	defer issuerKey.Destroy()

	cert := newCertificate(t.Id, s.MyId(), trusteeId, opts.Lvl, opts.Exp)

	inv, err := createInvitation(s.rand, secret, cert, ringKey, issuerKey, trusteeKey, t.Opts)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error generating invitation to trustee [%v] for  [%v]", trusteeId, t.Id)
	}

	if err := s.net.InvitationRegister(cancel, s.auth, inv); err != nil {
		return Invitation{}, errors.Wrapf(err, "Error registering invitation: %v", inv)
	}

	return inv, nil
}
