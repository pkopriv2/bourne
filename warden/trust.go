package warden

import (
	"fmt"
	"io"
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

// Only used for storage
type TrustCore struct {
	Id         uuid.UUID
	Name       string
	Opts       TrustOptions
	PubShard   SignedShard
	SigningKey SignedKeyPair
}

func (t TrustCore) asTrust(code TrustCode, cert SignedCertificate) Trust {
	return Trust{t.Id, t.Name, t.Opts, t.SigningKey, t.PubShard, cert, code.Shard}
}

func (t TrustCore) publicCore() TrustCore {
	return TrustCore{t.Id, t.Name, t.Opts, t.PubShard, SignedKeyPair{}}
}

// Only used for storage
type TrustCode struct {
	MemberId uuid.UUID
	TrustId  uuid.UUID
	Shard    SignedEncryptedShard
}

// General trust options.
type TrustOptions struct {
	Secret SecretOptions

	// Invitation options
	InvitationKey KeyExchangeOptions

	// signature options
	SigningHash Hash

	// default key options.
	SigningKey KeyPairOptions
}

func buildTrustOptions(fns ...func(*TrustOptions)) TrustOptions {
	ret := TrustOptions{buildSecretOptions(), buildKeyExchangeOpts(), SHA256, buildKeyPairOptions()}
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
	trustSigningKey SignedKeyPair

	// the public portion of the shared secret
	trustShard SignedShard

	// the certificate affirming the caller's relationship with the trust
	trusteeCert SignedCertificate

	// the private shard portion of the shared secret. (may only be unlocked by owner)
	trusteeShard SignedEncryptedShard
}

// generates a trust, but has no server-side effects.
func newTrust(rand io.Reader, myId uuid.UUID, mySecret Secret, mySigningKey Signer, name string, fns ...func(s *TrustOptions)) (Trust, error) {
	opts := buildTrustOptions(fns...)

	trustSigningKey, err := opts.SigningKey.Algorithm.Gen(rand, opts.SigningKey.Strength)
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error generating key [%v]: %v", opts.SigningKey.Algorithm, opts.SigningKey.Strength)
	}
	defer trustSigningKey.Destroy()

	// generate the core trust secret
	trustSecret, err := genSecret(rand, opts.Secret)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer trustSecret.Destroy()

	// generate the core trust encryption key (all shared items encrypted with this)
	trustEncryptionKey, err := trustSecret.Hash(opts.Secret.SecretHash)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer cryptoBytes(trustEncryptionKey).Destroy()

	// generate the public shard of the trust's secret
	trustShard, err := trustSecret.Shard(rand)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	// generate the member's shard of the trust's secret
	myShard, err := trustSecret.Shard(rand)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer myShard.Destroy()

	// generate the encryption key from the secret. (all shared items encrypted with this as seed)
	myEncryptionKey, err := mySecret.Hash(opts.Secret.SecretHash)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	trustSignedShard, err := signShard(rand, mySigningKey, trustShard)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	myEncryptedShard, err := encryptShard(rand, mySigningKey, myShard, myEncryptionKey)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	trustEncryptedKey, err := encryptKey(rand, mySigningKey, trustSigningKey, trustEncryptionKey, opts.SigningKey)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	myCert := newCertificate(uuid.NewV1(), myId, myId, Creator, OneHundredYears)

	mySignedCert, err := signCertificate(
		rand, myCert, trustSigningKey, mySigningKey, mySigningKey, opts.SigningHash)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	return Trust{
		Id:              myCert.Trust,
		Name:            name,
		Opts:            opts,
		trustSigningKey: trustEncryptedKey,
		trustShard:      trustSignedShard,
		trusteeCert:     mySignedCert,
		trusteeShard:    myEncryptedShard,
	}, nil
}

// String form of the trust.
func (d Trust) String() string {
	return fmt.Sprintf("Trust(id=%v,name=%v): %v", d.Id, d.Name, d.trusteeCert)
}

// Extracts the  oracle curve.  Requires *Encrypt* level trust
func (d Trust) deriveSecret(mySecret Secret) (Secret, error) {
	if err := Encrypt.verify(d.trusteeCert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	myEncryptionKey, err := mySecret.Hash(d.Opts.Secret.SecretHash)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	myShard, err := d.trusteeShard.Decrypt(myEncryptionKey)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	secret, err := d.trustShard.Derive(myShard)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return secret, nil
}

func (d Trust) core() TrustCore {
	return TrustCore{d.Id, d.Name, d.Opts, d.trustShard, d.trustSigningKey}
}

func (d Trust) trusteeCode() TrustCode {
	return TrustCode{d.trusteeCert.Trustee, d.Id, d.trusteeShard}
}

func (t Trust) unlockEncryptionSeed(secret Secret) ([]byte, error) {
	key, err := secret.Hash(t.Opts.Secret.SecretHash)
	return key, errors.WithStack(err)
}

func (t Trust) unlockSigningKey(secret Secret) (PrivateKey, error) {
	if err := Sign.verify(t.trusteeCert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	key, err := t.unlockEncryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	priv, err := t.trustSigningKey.Decrypt(key)
	return priv, errors.WithStack(err)
}

func (t Trust) renewCertificate(cancel <-chan struct{}, s *Session) (Trust, error) {
	mySecret, err := s.mySecret()
	if err != nil {
		return Trust{}, errors.Wrap(err, "Error deriving trust secret.")
	}
	defer mySecret.Destroy()

	trustSecret, err := t.deriveSecret(mySecret)
	if err != nil {
		return Trust{}, errors.Wrap(err, "Error deriving trust secret.")
	}
	defer trustSecret.Destroy()

	trustSigningKey, err := t.unlockSigningKey(trustSecret)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer trustSigningKey.Destroy()

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

	cert := newCertificate(t.Id, s.MyId(), s.MyId(), t.trusteeCert.Level, t.trusteeCert.Duration())

	myCert, err := signCertificate(
		s.rand, cert, trustSigningKey, mySigningKey, mySigningKey, t.Opts.SigningHash)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	myShard, err := trustSecret.Shard(s.rand)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer myShard.Destroy()

	myShardEnc, err := encryptShard(s.rand, mySigningKey, myShard, myEncryptionSeed)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	trust := Trust{
		Id:              t.Id,
		Name:            t.Name,
		Opts:            t.Opts,
		trustSigningKey: t.trustSigningKey,
		trustShard:      t.trustShard,
		trusteeCert:     myCert,
		trusteeShard:    myShardEnc,
	}

	token, err := s.token(cancel)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	err = s.net.CertRegister(cancel, token, myCert, trust.trusteeCode())
	return trust, errors.Wrapf(err, "Error renewing subscriber [%v] cert to trust [%v]", s.MyId(), t.Id)
}

// Loads all the trust certificates that have been issued by this .
func (t Trust) listCertificates(cancel <-chan struct{}, s Session, fns ...func(*PagingOptions)) ([]Certificate, error) {
	if err := Verify.verify(t.trusteeCert.Level); err != nil {
		return nil, newLevelOfTrustError(Verify, t.trusteeCert.Level)
	}

	token, err := s.token(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	list, err := s.net.CertsByTrust(cancel, token, t.Id, buildPagingOptions(fns...))
	return list, errors.WithStack(err)
}

// Revokes all issued certificates by this  for the given subscriber.
func (t Trust) revokeCertificate(cancel <-chan struct{}, s *Session, trustee uuid.UUID) error {
	if err := Revoke.verify(t.trusteeCert.Level); err != nil {
		return newLevelOfTrustError(Invite, t.trusteeCert.Level)
	}

	token, err := s.token(cancel)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.Wrapf(s.net.CertRevoke(cancel, token, t.trusteeCert.Id),
		"Unable to revoke certificate [%v] for subscriber [%v]", t.trusteeCert.Id, trustee)
}

// Issues an invitation to the given key.
func (t Trust) invite(cancel <-chan struct{}, s *Session, trusteeId uuid.UUID, fns ...func(*InvitationOptions)) (Invitation, error) {
	if err := Invite.verify(t.trusteeCert.Level); err != nil {
		return Invitation{}, newLevelOfTrustError(Invite, t.trusteeCert.Level)
	}

	opts := buildInvitationOptions(fns...)

	token, err := s.token(cancel)
	if err != nil {
		return Invitation{}, errors.WithStack(err)
	}

	s.logger.Debug("Loading member key [%v]", trusteeId)

	trusteeKey, o, err := s.net.MemberInviteKeyById(cancel, token, trusteeId)
	if err != nil {
		return Invitation{}, errors.WithStack(err)
	}

	if !o {
		s.logger.Error("Error loading member key.  No such member [%v]", trusteeId, trusteeKey.Id())
		return Invitation{}, errors.Wrapf(UnknownMemberError, "No such member [%v]", trusteeId)
	}

	s.logger.Debug("Loaded member key [%v] : [%v]", trusteeId, trusteeKey.Id())

	mySecret, err := s.mySecret()
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to unlock  oracle [%v]", t.Id)
	}
	defer mySecret.Destroy()

	mySigningKey, err := s.mySigningKey(mySecret)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving my signing key [%v]", s.MyId())
	}
	defer mySigningKey.Destroy()

	trustSecret, err := t.deriveSecret(mySecret)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to unlock  oracle [%v]", t.Id)
	}
	defer trustSecret.Destroy()

	trustSigningKey, err := t.unlockSigningKey(trustSecret)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Erro retrieving signing key [%v]", t.Id)
	}
	defer trustSigningKey.Destroy()

	cert := newCertificate(t.Id, s.MyId(), trusteeId, opts.Lvl, opts.Exp)

	inv, err := createInvitation(s.rand, trustSecret, cert, trustSigningKey, mySigningKey, trusteeKey, t.Opts)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error generating invitation to trustee [%v] for  [%v]", trusteeId, t.Id)
	}

	token, err = s.token(cancel)
	if err != nil {
		return Invitation{}, errors.WithStack(err)
	}

	if err := s.net.InvitationRegister(cancel, token, inv); err != nil {
		return Invitation{}, errors.Wrapf(err, "Error registering invitation: %v", inv)
	}

	return inv, nil
}
