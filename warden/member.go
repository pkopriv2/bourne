package warden

import (
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// Strength is a single argument which describes the security
// requirements of the
type Strength int

const (
	Minimal Strength = 1024 * iota // bits of entropy.
	Strong
	Stronger
	Strongest
)

// Role maps the capabilities
type Role int

const (
	Basic Role = iota
	Manager
	Issuer
	Subscriber
	God
)

type MemberAgreement struct {
	MemberId     uuid.UUID
	SubscriberId uuid.UUID
	Role         Role
	Enabled      bool
	// Issued       time.Time
	// Expires      time.Time
}

func newMemberAgreement(memberId, subscriberId uuid.UUID, role Role) MemberAgreement {
	return MemberAgreement{memberId, subscriberId, role, true}
}

type memberOptions struct {
	Secret        secretOptions
	InviteKey     KeyPairOptions
	SigningKey    KeyPairOptions
	SignatureHash Hash
	NonceSize     int
}

func (s *memberOptions) SecretOptions(fn func(*secretOptions)) {
	s.Secret = buildSecretOptions(fn)
}

func (s *memberOptions) SigningOptions(fn func(*KeyPairOptions)) {
	s.SigningKey = buildKeyPairOptions(fn)
}

func (s *memberOptions) InviteOptions(fn func(*KeyPairOptions)) {
	s.InviteKey = buildKeyPairOptions(fn)
}

func buildMemberOptions(fns ...func(*memberOptions)) memberOptions {
	ret := memberOptions{buildSecretOptions(), buildKeyPairOptions(), buildKeyPairOptions(), SHA256, 16}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A MemberCore contains all the membership details of a particular user.
type memberCore struct {
	Id             uuid.UUID
	SubscriptionId uuid.UUID
	Pub            SignedShard
	SigningKey     SignedKeyPair
	InviteKey      SignedKeyPair
	Opts           memberOptions
}

// Generates a new member.  Rand and creds must not be nil!
func newMember(rand io.Reader, id, subId uuid.UUID, creds credential, fns ...func(*memberOptions)) (memberCore, memberShard, error) {
	opts := buildMemberOptions(fns...)
	if rand == nil || creds == nil {
		return memberCore{}, memberShard{}, errors.WithStack(common.ArgError)
	}

	// generate the user's secret.
	secret, err := genSecret(rand, opts.Secret)
	if err != nil {
		return memberCore{}, memberShard{}, errors.WithStack(err)
	}
	defer secret.Destroy()

	// generate the member's encryption seed
	encSeed, err := secret.Hash(opts.Secret.SecretHash)
	if err != nil {
		return memberCore{}, memberShard{}, errors.WithStack(err)
	}
	defer cryptoBytes(encSeed).Destroy()

	// generate the member's public shard (will be returned)
	pubShard, err := secret.Shard(rand)
	if err != nil {
		return memberCore{}, memberShard{}, errors.WithStack(err)
	}

	// generate the member's private shard (will return via MemberShard)
	privShard, err := secret.Shard(rand)
	if err != nil {
		return memberCore{}, memberShard{}, errors.WithStack(err)
	}

	rawSigningKey, err := opts.SigningKey.Algorithm.Gen(rand, opts.SigningKey.Strength)
	if err != nil {
		return memberCore{}, memberShard{}, errors.WithStack(err)
	}
	defer rawSigningKey.Destroy()

	rawInviteKey, err := opts.InviteKey.Algorithm.Gen(rand, opts.InviteKey.Strength)
	if err != nil {
		return memberCore{}, memberShard{}, errors.WithStack(err)
	}
	defer rawInviteKey.Destroy()

	// for now, just self sign.
	encSigningKey, err := encryptKey(rand, rawSigningKey, rawSigningKey, encSeed, opts.SigningKey)
	if err != nil {
		return memberCore{}, memberShard{}, errors.WithStack(err)
	}

	encInviteKey, err := encryptKey(rand, rawSigningKey, rawInviteKey, encSeed, opts.InviteKey)
	if err != nil {
		return memberCore{}, memberShard{}, errors.WithStack(err)
	}

	sigPubShard, err := signShard(rand, rawSigningKey, pubShard)
	if err != nil {
		return memberCore{}, memberShard{}, errors.WithStack(err)
	}

	encShard, err := creds.EncryptShard(rand, rawSigningKey, privShard)
	if err != nil {
		return memberCore{}, memberShard{}, errors.WithStack(err)
	}

	return memberCore{
		Id:             id,
		SubscriptionId: subId,
		Pub:            sigPubShard,
		SigningKey:     encSigningKey,
		InviteKey:      encInviteKey,
		Opts:           opts,
	}, encShard, nil
}

func (s memberCore) secret(shard memberShard, login func() credential) (Secret, error) {
	creds := login()
	defer creds.Destroy()

	priv, err := creds.DecryptShard(shard)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	secret, err := s.Pub.Derive(priv)
	return secret, errors.WithStack(err)
}

func (s memberCore) encryptionSeed(secret Secret) ([]byte, error) {
	key, err := secret.Hash(s.Opts.SignatureHash)
	return key, errors.WithStack(err)
}

func (s memberCore) signingKey(secret Secret) (PrivateKey, error) {
	key, err := s.encryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	signingKey, err := s.SigningKey.Decrypt(key)
	return signingKey, errors.WithStack(err)
}

func (s memberCore) invitationKey(secret Secret) (PrivateKey, error) {
	key, err := s.encryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	inviteKey, err := s.InviteKey.Decrypt(key)
	return inviteKey, errors.WithStack(err)
}

func (m memberCore) newShard(rand io.Reader, secret Secret, login func() credential) (memberShard, error) {
	creds := login()
	defer creds.Destroy()

	shard, err := secret.Shard(rand)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}
	defer shard.Destroy()

	signer, err := m.signingKey(secret)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	enc, err := creds.EncryptShard(rand, signer, shard)
	return enc, errors.WithStack(err)
}
