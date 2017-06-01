package warden

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const OneHundredYears = 100 * 365 * 24 * time.Hour

// General trust options.
type trustOptions struct {
	Secret        secretOptions
	InvitationKey KeyExchangeOptions
	SigningHash   Hash
	SigningKey    KeyPairOptions
}

func buildTrustOptions(fns ...func(*trustOptions)) trustOptions {
	ret := trustOptions{buildSecretOptions(), buildKeyExchangeOpts(), SHA256, buildKeyPairOptions()}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A trust is a repository of data that has been entrusted to one or
// many individuals.  For all intents and purposes, they behave very much
// like a legal trust: someone wishes to outsource the management of a
// resource, but only wishes to do so with legally binding terms.
//
// A digital trust behaves very similarily, except, the terms are enforced
// through the use of knowledge partitioning and a trusted 3rd party.  Only
// valid members may rederive the trust's shared secret, while the trusted
// 3rd party enforces the distribution and visibility of the trust.
//
type Trust struct {
	Id uuid.UUID

	// the options of the trust
	opts trustOptions

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
func newTrust(rand io.Reader, myId uuid.UUID, mySecret Secret, mySigningKey Signer, fns ...func(s *trustOptions)) (Trust, error) {
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

	myEncryptedShard, err := encryptAndSignShard(rand, mySigningKey, myShard, myEncryptionKey)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	trustEncryptedKey, err := encryptKey(rand, mySigningKey, trustSigningKey, trustEncryptionKey, opts.SigningKey)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	myCert := newCertificate(uuid.NewV1(), myId, myId, Owner, OneHundredYears)

	mySignedCert, err := signCertificate(
		rand, myCert, trustSigningKey, mySigningKey, mySigningKey, opts.SigningHash)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	return Trust{
		Id:              myCert.TrustId,
		opts:            opts,
		trustSigningKey: trustEncryptedKey,
		trustShard:      trustSignedShard,
		trusteeCert:     mySignedCert,
		trusteeShard:    myEncryptedShard,
	}, nil
}

// String form of the trust.
func (d Trust) String() string {
	key := "<Unauthorized>"
	if Beneficiary.MetBy(d.trusteeCert.Level) {
		key = d.trustSigningKey.Pub.Id()
	}

	return fmt.Sprintf("Trust(id=%v, key=%v): %v", formatUUID(d.Id), key, d.trusteeCert)
}

// Rederives the trust secret.
func (d Trust) deriveSecret(mySecret Secret) (Secret, error) {
	if !Beneficiary.MetBy(d.trusteeCert.Level) {
		return nil, errors.WithStack(UnauthorizedError)
	}

	myEncryptionKey, err := mySecret.Hash(d.opts.Secret.SecretHash)
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

func (d Trust) core() trustCore {
	return trustCore{d.Id, d.opts, d.trustShard, d.trustSigningKey}
}

func (d Trust) trusteeCode() trustCode {
	return trustCode{d.trusteeCert.TrusteeId, d.Id, d.trusteeShard}
}

func (t Trust) unlockEncryptionSeed(secret Secret) ([]byte, error) {
	if !Beneficiary.MetBy(t.trusteeCert.Level) {
		return nil, errors.WithStack(UnauthorizedError)
	}

	key, err := secret.Hash(t.opts.Secret.SecretHash)
	return key, errors.WithStack(err)
}

func (t Trust) unlockSigningKey(secret Secret) (PrivateKey, error) {
	if !Manager.MetBy(t.trusteeCert.Level) {
		return nil, errors.WithStack(UnauthorizedError)
	}

	key, err := t.unlockEncryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	priv, err := t.trustSigningKey.Decrypt(key)
	return priv, errors.WithStack(err)
}

func (t Trust) renewCertificate(cancel <-chan struct{}, s *session) (Trust, error) {
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
		s.rand, cert, trustSigningKey, mySigningKey, mySigningKey, t.opts.SigningHash)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	myShard, err := trustSecret.Shard(s.rand)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer myShard.Destroy()

	myShardEnc, err := encryptAndSignShard(s.rand, mySigningKey, myShard, myEncryptionSeed)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	trust := Trust{
		Id:              t.Id,
		opts:            t.opts,
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
func (t Trust) listCertificates(cancel <-chan struct{}, s session, fns ...func(*PagingOptions)) ([]SignedCertificate, error) {
	if !Manager.MetBy(t.trusteeCert.Level) {
		return nil, errors.WithStack(UnauthorizedError)
	}

	token, err := s.token(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	list, err := s.net.CertsByTrust(cancel, token, t.Id, buildPagingOptions(fns...))
	return list, errors.WithStack(err)
}

// Revokes all issued certificates by this  for the given subscriber.
func (t Trust) revokeCertificate(cancel <-chan struct{}, s *session, trusteeId uuid.UUID) error {
	if !Director.MetBy(t.trusteeCert.Level) {
		return errors.WithStack(UnauthorizedError)
	}

	token, err := s.token(cancel)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(s.net.CertRevoke(cancel, token, trusteeId, t.Id))
}

// Issues an invitation to the given key.
func (t Trust) invite(cancel <-chan struct{}, s *session, trusteeId uuid.UUID, fns ...func(*InvitationOptions)) (Invitation, error) {
	if !Director.MetBy(t.trusteeCert.Level) {
		return Invitation{}, errors.WithStack(UnauthorizedError)
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

	inv, err := createInvitation(s.rand, trustSecret, cert, trustSigningKey, mySigningKey, trusteeKey, t.opts)
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

// Only used for storage
type trustCore struct {
	Id         uuid.UUID
	Opts       trustOptions
	PubShard   SignedShard
	SigningKey SignedKeyPair
}

func (t trustCore) privateTrust(code trustCode, cert SignedCertificate) Trust {
	return Trust{t.Id, t.Opts, t.SigningKey, t.PubShard, cert, code.Shard}
}

func (t trustCore) publicCore() trustCore {
	return trustCore{t.Id, t.Opts, t.PubShard, SignedKeyPair{KeyPair: KeyPair{Pub: t.SigningKey.Pub}}}
}

func (t trustCore) String() string {
	return fmt.Sprintf("Core(trustId=%v) : %v", formatUUID(t.Id), t.SigningKey.Pub.Id())
}

// Only used for storage
type trustCode struct {
	MemberId uuid.UUID
	TrustId  uuid.UUID
	Shard    SignedEncryptedShard
}

func (t trustCode) String() string {
	return fmt.Sprintf("Code(memberId=%v, trustId=%v) : %v", formatUUID(t.MemberId), formatUUID(t.TrustId), t.Shard.Sig)
}
