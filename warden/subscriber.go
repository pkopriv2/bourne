package warden

import (
	"io"
	"math/rand"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type SubscriberOptions struct {
	Secret        SecretOptions
	InviteKey     KeyPairOptions
	SigningKey    KeyPairOptions
	SignatureHash Hash
	NonceSize     int
}

func (s *SubscriberOptions) SecretOptions(fn func(*SecretOptions)) {
	s.Secret = buildSecretOptions(fn)
}

func (s *SubscriberOptions) SigningOptions(fn func(*KeyPairOptions)) {
	s.SigningKey = buildKeyPairOptions(fn)
}

func (s *SubscriberOptions) InviteOptions(fn func(*KeyPairOptions)) {
	s.InviteKey = buildKeyPairOptions(fn)
}

func buildSubscriberOptions(fns ...func(*SubscriberOptions)) SubscriberOptions {
	ret := SubscriberOptions{buildSecretOptions(), buildKeyPairOptions(), buildKeyPairOptions(), SHA256, 16}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

type AccessShard interface {
	Derive(pub Shard, pad *oneTimePad) (Secret, error)
}

type Subscriber struct {
	Id         uuid.UUID
	Pub        SignedShard
	SigningKey SignedKeyPair
	InviteKey  SignedKeyPair
	Opts       SubscriberOptions
}

func (s Subscriber) mySecret(auth AccessShard, login func(KeyPad) error) (Secret, error) {
	creds, err := enterCreds(login)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	secret, err := auth.Derive(s.Pub, creds)
	return secret, errors.WithStack(err)
}

func (s Subscriber) myEncryptionSeed(secret Secret) ([]byte, error) {
	key, err := secret.Hash(s.Opts.SignatureHash)
	return key, errors.WithStack(err)
}

func (s Subscriber) mySigningKey(secret Secret) (PrivateKey, error) {
	key, err := s.myEncryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	signingKey, err := s.SigningKey.Decrypt(key)
	return signingKey, errors.WithStack(err)
}

func (s Subscriber) myInvitationKey(secret Secret) (PrivateKey, error) {
	key, err := s.myEncryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	inviteKey, err := s.InviteKey.Decrypt(key)
	return inviteKey, errors.WithStack(err)
}

func NewSubscriber(rand io.Reader, pad *oneTimePad, fns ...func(*SubscriberOptions)) (Subscriber, AccessShard, error) {
	opts := buildSubscriberOptions(fns...)

	// generate all the secret data.
	secret, err := genSecret(rand, opts.Secret)
	if err != nil {
		return Subscriber{}, nil, errors.WithStack(err)
	}
	defer secret.Destroy()

	secretKey, err := secret.Hash(opts.Secret.SecretHash)
	if err != nil {
		return Subscriber{}, nil, errors.WithStack(err)
	}
	defer cryptoBytes(secretKey).Destroy()

	rawPubShard, err := secret.Shard(rand)
	if err != nil {
		return Subscriber{}, nil, errors.WithStack(err)
	}

	rawSigningKey, err := opts.SigningKey.Algorithm.Gen(rand, opts.SigningKey.Strength)
	if err != nil {
		return Subscriber{}, nil, errors.WithStack(err)
	}
	defer rawSigningKey.Destroy()

	rawInviteKey, err := opts.InviteKey.Algorithm.Gen(rand, opts.InviteKey.Strength)
	if err != nil {
		return Subscriber{}, nil, errors.WithStack(err)
	}
	defer rawInviteKey.Destroy()

	// for now, just self sign.
	encSigningKey, err := encryptKey(rand, rawSigningKey, rawSigningKey, secretKey, opts.SigningKey)
	if err != nil {
		return Subscriber{}, nil, errors.WithStack(err)
	}

	encInviteKey, err := encryptKey(rand, rawSigningKey, rawInviteKey, secretKey, opts.InviteKey)
	if err != nil {
		return Subscriber{}, nil, errors.WithStack(err)
	}

	sigPubShard, err := signShard(rand, rawSigningKey, rawPubShard)
	if err != nil {
		return Subscriber{}, nil, errors.WithStack(err)
	}

	var auth AccessShard
	if pad.Signer != nil {
		auth, err = newSignatureAuth(rand, secret, pad, opts)
		if err != nil {
			return Subscriber{}, nil, errors.WithStack(err)
		}
	}

	if auth == nil {
		return Subscriber{}, nil, errors.Wrap(UnsupportedLoginError, "Must provide at least one login method")
	}

	return Subscriber{
		Id:         uuid.NewV1(),
		Pub:        sigPubShard,
		SigningKey: encSigningKey,
		InviteKey:  encInviteKey,
		Opts:       opts,
	}, auth, nil
}

// Authenticator implementations.

// Signature based authenticator.  This assumes that the signer in question.
type SignatureShard struct {
	Nonce []byte
	Hash  Hash
	Priv  SignedEncryptedShard
}

func newSignatureAuth(random io.Reader, secret Secret, pad *oneTimePad, opts SubscriberOptions) (SignatureShard, error) {
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

	return SignatureShard{nonce, opts.SignatureHash, shardEnc}, nil
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
