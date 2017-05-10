package warden

import (
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const OneHundredYears = 100 * 365 * 24 * time.Hour

// General trust options.
type TrustOptions struct {
	SecretOptions

	// invitation options
	InvitationCipher SymmetricCipher
	InvitationHash   Hash
	InvitationIter   int // used for key derivations only

	// signature options
	SignatureHash Hash

	// default key options.
	KeyOpts KeyPairOptions
}

func buildTrustOptions(fns ...func(*TrustOptions)) TrustOptions {
	ret := TrustOptions{buildSecretOptions(), Aes256Gcm, SHA256, 1024, SHA256, buildKeyPairOptions()}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A trust is a personal view of a digital trust.  Fields will be populated
// based on the subscriber's relationship with the trust.
type Trust struct {
	Id uuid.UUID

	// the public signing key of the trust.
	Pub PublicKey

	// the session owner's certificate with the trust.
	Cert SignedCertificate

	// the options of the trust
	Opts TrustOptions

	// the public shard portion of the shared secret.
	pubShard signedShard

	// the private shard portion of the shared secret. (may only be unlocked by owner)
	privShard signedEncryptedShard
}

// generates a trust, but has no server-side effects.
func generateTrust(s *Session, name string, fns ...func(s *TrustOptions)) (Trust, SignedKeyPair, error) {
	opts := buildTrustOptions(fns...)

	mySigningKey, err := s.mySigningKey()
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.Wrapf(err,
			"Error extracting session signing key [%v]", s.MyId())
	}
	defer mySigningKey.Destroy()

	trustSigningKey, err := opts.KeyOpts.Algorithm.Gen(s.rand, opts.KeyOpts.Strength)
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.Wrapf(err,
			"Error generating  key [%v]: %v", opts.KeyOpts.Algorithm, opts.KeyOpts.Strength)
	}
	defer trustSigningKey.Destroy()

	secret, err := genSecret(s.rand, opts.SecretOptions)
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.WithStack(err)
	}
	defer secret.Destroy()

	pubShard, err := secret.Shard(s.rand)
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.WithStack(err)
	}
	defer pubShard.Destroy()

	privShard, err := secret.Shard(s.rand)
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.WithStack(err)
	}
	defer privShard.Destroy()

	signedShard, err := signShard(s.rand, mySigningKey, pubShard)
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.WithStack(err)
	}

	encShard, err := encryptShard(s.rand, mySigningKey, privShard, s.myOracle())
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.WithStack(err)
	}

	pass, err := secret.Format()
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.WithStack(err)
	}
	defer cryptoBytes(pass).Destroy()

	pair, err := genKeyPair(s.rand, trustSigningKey, pass, opts.KeyOpts)
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.WithStack(err)
	}

	cert := newCertificate(uuid.NewV1(), s.MyId(), s.MyId(), Creator, OneHundredYears)

	signedPair, err := pair.Sign(s.rand, mySigningKey, opts.SignatureHash)
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.WithStack(err)
	}

	signedCert, err := signCertificate(
		s.rand, cert, trustSigningKey, mySigningKey, mySigningKey, opts.SignatureHash)
	if err != nil {
		return Trust{}, SignedKeyPair{}, errors.WithStack(err)
	}

	return Trust{
		cert.Trust,
		trustSigningKey.Public(),
		signedCert,
		opts,
		signedShard,
		encShard,
	}, signedPair, nil
}

// Extracts the  oracle curve.  Requires *Encrypt* level trust
func (d Trust) unlockSecret(s *Session) (Secret, error) {
	if err := Encrypt.verify(d.Cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	myShard, err := d.privShard.Decrypt(s.myOracle())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	secret, err := d.pubShard.Derive(myShard)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return secret, nil
}

func (d Trust) unlockSigningKey(cancel <-chan struct{}, s *Session, secret Secret) (PrivateKey, error) {
	if err := Sign.verify(d.Cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	// key, err := secret.Format()
	// if err != nil {
	// return nil, errors.WithStack(err)
	// }

	return nil, nil
}

func (d Trust) renewCertificate(cancel <-chan struct{}, s *Session) (Trust, error) {
	if err := Invite.verify(d.Cert.Level); err != nil {
		return Trust{}, errors.WithStack(err)
	}

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

	mySigningKey, err := s.mySigningKey()
	if err != nil {
		return Trust{}, errors.Wrapf(err, "Error extracting session signing key [%v]", s.MyId())
	}
	defer mySigningKey.Destroy()

	cert := newCertificate(d.Id, s.MyId(), s.MyId(), d.Cert.Level, d.Cert.Duration())

	myCert, err := signCertificate(
		s.rand, cert, trustSigningKey, mySigningKey, mySigningKey, d.Opts.SignatureHash)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	shard, err := secret.Shard(s.rand)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	myShard, err := encryptShard(s.rand, mySigningKey, shard, s.myOracle())
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	if err := s.net.Certs.Register(cancel, s.auth, myCert, myShard); err != nil {
		return Trust{}, errors.Wrapf(err, "Error renewing subscriber [%v] cert to trust [%v]", s.MyId(), d.Id)
	}

	return Trust{
		d.Id,
		d.Pub,
		myCert,
		d.Opts,
		d.pubShard,
		myShard,
	}, nil
}

// Loads all the trust certificates that have been issued by this .
func (t Trust) listCertificates(cancel <-chan struct{}, s Session, fns ...func(*PagingOptions)) ([]Certificate, error) {
	if err := Verify.verify(t.Cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	opts := buildPagingOptions(fns...)
	return s.net.Certs.ActiveByTrust(cancel, s.auth, t.Id, opts.Beg, opts.End)
}

// Revokes all issued certificates by this  for the given subscriber.
func (t Trust) revokeCertificate(cancel <-chan struct{}, s *Session, trustee uuid.UUID) error {
	if err := Revoke.verify(t.Cert.Level); err != nil {
		return errors.WithStack(err)
	}

	if err := s.net.Certs.Revoke(cancel, s.auth, t.Cert.Id); err != nil {
		return errors.Wrapf(err, "Unable to revoke certificate [%v] for subscriber [%v]", t.Cert.Id, trustee)
	}

	return nil
}

// Issues an invitation to the given key.
func (t Trust) invite(cancel <-chan struct{}, s *Session, trustee Trust, fns ...func(*InvitationOptions)) (Invitation, error) {
	if err := Invite.verify(t.Cert.Level); err != nil {
		return Invitation{}, newLevelOfTrustError(Invite, t.Cert.Level)
	}

	opts := buildInvitationOptions(fns...)

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

	issuerKey, err := s.mySigningKey()
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving my signing key [%v]", s.MyId())
	}
	defer issuerKey.Destroy()

	cert := newCertificate(t.Id, s.MyId(), trustee.Id, opts.Lvl, opts.Exp)

	inv, err := createInvitation(s.rand, secret, cert, ringKey, issuerKey, trustee.Pub, t.Opts)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error generating invitation to trustee [%v] for  [%v]", trustee, t.Id)
	}

	if err := s.net.Invites.Upload(cancel, s.auth, inv); err != nil {
		return Invitation{}, errors.Wrapf(err, "Error registering invitation: %v", inv)
	}

	return inv, nil
}
