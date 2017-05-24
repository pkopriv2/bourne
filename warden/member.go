package warden

import (
	"io"
	"math/rand"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

type MemberOptions struct {
	Secret        SecretOptions
	InviteKey     KeyPairOptions
	SigningKey    KeyPairOptions
	SignatureHash Hash
	NonceSize     int
}

func (s *MemberOptions) SecretOptions(fn func(*SecretOptions)) {
	s.Secret = buildSecretOptions(fn)
}

func (s *MemberOptions) SigningOptions(fn func(*KeyPairOptions)) {
	s.SigningKey = buildKeyPairOptions(fn)
}

func (s *MemberOptions) InviteOptions(fn func(*KeyPairOptions)) {
	s.InviteKey = buildKeyPairOptions(fn)
}

func buildMemberOptions(fns ...func(*MemberOptions)) MemberOptions {
	ret := MemberOptions{buildSecretOptions(), buildKeyPairOptions(), buildKeyPairOptions(), SHA256, 16}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A MemberCode is an access code that ties a particular login to a member account.
type MemberCode struct {
	MemberShard
	MemberId uuid.UUID
}

// A MemberShard is a secret shard (See Shard) that is specific to a member and
// allows a member to rederive his/her secret.
type MemberShard interface {
	Lookup() []byte // must be globally unique.
	Derive(pub Shard, pad *oneTimePad) (Secret, error)
}

// A MemberCore contains all the membership details of a particular user.
type MemberCore struct {
	Id         uuid.UUID
	Pub        SignedShard
	SigningKey SignedKeyPair
	InviteKey  SignedKeyPair
	Opts       MemberOptions
}

func newMember(rand io.Reader, pad *oneTimePad, fns ...func(*MemberOptions)) (MemberCore, MemberCode, error) {
	opts := buildMemberOptions(fns...)

	// generate the user's secret.
	secret, err := genSecret(rand, opts.Secret)
	if err != nil {
		return MemberCore{}, MemberCode{}, errors.WithStack(err)
	}
	defer secret.Destroy()

	secretKey, err := secret.Hash(opts.Secret.SecretHash)
	if err != nil {
		return MemberCore{}, MemberCode{}, errors.WithStack(err)
	}
	defer cryptoBytes(secretKey).Destroy()

	rawShard, err := secret.Shard(rand)
	if err != nil {
		return MemberCore{}, MemberCode{}, errors.WithStack(err)
	}

	rawSigningKey, err := opts.SigningKey.Algorithm.Gen(rand, opts.SigningKey.Strength)
	if err != nil {
		return MemberCore{}, MemberCode{}, errors.WithStack(err)
	}
	defer rawSigningKey.Destroy()

	rawInviteKey, err := opts.InviteKey.Algorithm.Gen(rand, opts.InviteKey.Strength)
	if err != nil {
		return MemberCore{}, MemberCode{}, errors.WithStack(err)
	}
	defer rawInviteKey.Destroy()

	// for now, just self sign.
	encSigningKey, err := encryptKey(rand, rawSigningKey, rawSigningKey, secretKey, opts.SigningKey)
	if err != nil {
		return MemberCore{}, MemberCode{}, errors.WithStack(err)
	}

	encInviteKey, err := encryptKey(rand, rawSigningKey, rawInviteKey, secretKey, opts.InviteKey)
	if err != nil {
		return MemberCore{}, MemberCode{}, errors.WithStack(err)
	}

	sigPubShard, err := signShard(rand, rawSigningKey, rawShard)
	if err != nil {
		return MemberCore{}, MemberCode{}, errors.WithStack(err)
	}

	var auth MemberShard
	if pad.Signer != nil {
		auth, err = newSignatureShard(rand, secret, pad, opts)
		if err != nil {
			return MemberCore{}, MemberCode{}, errors.WithStack(err)
		}
	}

	if auth == nil {
		return MemberCore{}, MemberCode{}, errors.Wrap(UnsupportedLoginError, "Must provide at least one login method")
	}

	id := uuid.NewV1()
	return MemberCore{
		Id:         id,
		Pub:        sigPubShard,
		SigningKey: encSigningKey,
		InviteKey:  encInviteKey,
		Opts:       opts,
	}, MemberCode{auth, id}, nil
}

func (s MemberCore) secret(auth MemberShard, login func(KeyPad) error) (Secret, error) {
	creds, err := enterCreds(login)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	secret, err := auth.Derive(s.Pub, creds)
	return secret, errors.WithStack(err)
}

func (s MemberCore) encryptionSeed(secret Secret) ([]byte, error) {
	key, err := secret.Hash(s.Opts.SignatureHash)
	return key, errors.WithStack(err)
}

func (s MemberCore) signingKey(secret Secret) (PrivateKey, error) {
	key, err := s.encryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	signingKey, err := s.SigningKey.Decrypt(key)
	return signingKey, errors.WithStack(err)
}

func (s MemberCore) invitationKey(secret Secret) (PrivateKey, error) {
	key, err := s.encryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	inviteKey, err := s.InviteKey.Decrypt(key)
	return inviteKey, errors.WithStack(err)
}

func (m MemberCore) newCode(rand io.Reader, secret Secret, login func(KeyPad) error) (MemberCode, error) {
	pad, err := enterCreds(login)
	if err != nil {
		return MemberCode{}, errors.WithStack(err)
	}

	var auth MemberShard
	if pad.Signer != nil {
		auth, err = newSignatureShard(rand, secret, pad, m.Opts)
		if err != nil {
			return MemberCode{}, errors.WithStack(err)
		}
	}

	if auth == nil {
		return MemberCode{}, errors.Wrap(UnsupportedLoginError, "Must provide at least one login method")
	}

	return MemberCode{auth, m.Id}, nil
}

// Authenticator implementations.

// Signature based authenticator.
type SignatureShard struct {
	Pub   PublicKey
	Nonce []byte
	Hash  Hash
	Priv  SignedEncryptedShard
}

func newSignatureShard(random io.Reader, secret Secret, pad *oneTimePad, opts MemberOptions) (SignatureShard, error) {
	if pad.Signer == nil {
		return SignatureShard{}, errors.Wrap(UnsupportedLoginError, "Expected a signature based login")
	}

	nonce, err := genRandomBytes(random, opts.NonceSize)
	if err != nil {
		return SignatureShard{}, errors.WithStack(err)
	}

	// We must use a stable random source for the signature based keys.
	sig, err := pad.Signer.Sign(rand.New(rand.NewSource(0)), opts.Secret.SecretHash, nonce)
	if err != nil {
		return SignatureShard{}, errors.WithStack(err)
	}
	defer destroyBytes(sig.Data)

	shard, err := secret.Shard(random)
	if err != nil {
		return SignatureShard{}, errors.WithStack(err)
	}
	defer shard.Destroy()

	shardEnc, err := encryptShard(random, pad.Signer, shard, sig.Data)
	if err != nil {
		return SignatureShard{}, errors.WithStack(err)
	}

	return SignatureShard{Pub: pad.Signer.Public(), Nonce: nonce, Hash: opts.SignatureHash, Priv: shardEnc}, nil
}

func (s SignatureShard) Derive(pub Shard, pad *oneTimePad) (Secret, error) {
	if pad.Signer == nil {
		return nil, errors.Wrap(UnsupportedLoginError, "Expected a signature based login")
	}

	sig, err := pad.Signer.Sign(rand.New(rand.NewSource(0)), s.Hash, s.Nonce)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	priv, err := s.Priv.Decrypt(sig.Data)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	secret, err := pub.Derive(priv)
	return secret, errors.WithStack(err)
}

func (s SignatureShard) Lookup() []byte {
	return lookupByKey(s.Pub)
}

func lookupByKey(key PublicKey) []byte {
	return stash.String("SIG:/").ChildString(key.Id())
}
