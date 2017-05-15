package warden

import (
	"io"

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

type Subscriber struct {
	Id      uuid.UUID
	Creator PublicKey
	Nonce   []byte
	Shard   signedShard
	Sign    SignedKeyPair
	Invite  SignedKeyPair
}

func NewSubscriber(rand io.Reader, creator Signer, pass []byte, fns ...func(*SubscriberOptions)) (Subscriber, signedEncryptedShard, error) {
	opts := buildSubscriberOptions(fns...)

	nonce, err := genRandomBytes(rand, opts.NonceSize)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}

	secret, err := genSecret(rand, opts.Secret)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}
	defer secret.Destroy()

	secretKey, err := secret.Hash(SHA256)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}
	defer cryptoBytes(secretKey).Destroy()

	rawPrivShard, err := secret.Shard(rand)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}
	defer rawPrivShard.Destroy()

	rawPubShard, err := secret.Shard(rand)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}

	rawSigningKey, err := opts.SigningKey.Algorithm.Gen(rand, opts.SigningKey.Strength)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}
	defer rawSigningKey.Destroy()

	rawInviteKey, err := opts.InviteKey.Algorithm.Gen(rand, opts.InviteKey.Strength)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}
	defer rawInviteKey.Destroy()

	encSigningKey, err := encryptKey(rand, rawSigningKey, rawSigningKey, secretKey, opts.SigningKey)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}

	encInviteKey, err := encryptKey(rand, rawSigningKey, rawInviteKey, secretKey, opts.InviteKey)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}

	encPrivShard, err := encryptShard(rand, rawSigningKey, rawPrivShard, pass)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}

	sigPubShard, err := signShard(rand, rawSigningKey, rawPubShard)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}

	sigInviteKey, err := encInviteKey.Sign(rand, rawSigningKey, opts.SignatureHash)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}

	// if subscriber opted out of signature based registration, then just self sign.
	if creator == nil {
		creator = rawSigningKey
	}

	sigSigningKey, err := encSigningKey.Sign(rand, creator, opts.SignatureHash)
	if err != nil {
		return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	}

	return Subscriber{uuid.NewV1(), creator.Public(), nonce, sigPubShard, sigSigningKey, sigInviteKey}, encPrivShard, nil
}
