package warden

import (
	"io"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type SubscriberOptions struct {
	Oracle SecretOptions
	Invite KeyPairOptions
	Sign   KeyPairOptions
}

func (s *SubscriberOptions) InviteOptions(fn func(*KeyPairOptions)) {
	s.Invite = buildKeyPairOptions(fn)
}

func buildSubscriberOptions(fns ...func(*SubscriberOptions)) SubscriberOptions {
	ret := SubscriberOptions{buildSecretOptions(), buildKeyPairOptions(), buildKeyPairOptions()}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

type Subscriber struct {
	Id     uuid.UUID
	Oracle signedPublicShard
	Sign   SignedKeyPair
	Invite SignedKeyPair
}

func NewSubscriber(random io.Reader, pass []byte, fns ...func(*SubscriberOptions)) (Subscriber, signedPrivateShard, error) {
	opts := buildSubscriberOptions(fns...)

	oracle, line, err := genSharedSecret(random, opts.Oracle)
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}
	defer line.Destroy()

	secretKey, err := line.Format()
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}
	defer cryptoBytes(secretKey).Destroy()

	sign, err := opts.Sign.Algorithm.Gen(random, opts.Invite.Strength)
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}
	defer sign.Destroy()

	invite, err := opts.Invite.Algorithm.Gen(random, opts.Invite.Strength)
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}
	defer invite.Destroy()

	oracleKey, err := genPrivateShard(random, line, pass, opts.Oracle)
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}

	signKey, err := genKeyPair(random, sign, secretKey, opts.Invite)
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}

	inviteKey, err := genKeyPair(random, invite, secretKey, opts.Invite)
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}

	oracleSig, err := oracle.Sign(random, sign, opts.Sign.Hash)
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}

	oracleKeySig, err := oracleKey.Sign(random, sign, opts.Sign.Hash)
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}

	// TODO: this is self signed..don't think that matters
	signedSignKey, err := signKey.Sign(random, sign, opts.Sign.Hash)
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}

	signedInviteKey, err := inviteKey.Sign(random, sign, opts.Sign.Hash)
	if err != nil {
		return Subscriber{}, signedPrivateShard{}, errors.WithStack(err)
	}

	return Subscriber{uuid.NewV1(), oracleSig, signedSignKey, signedInviteKey}, oracleKeySig, nil
}
