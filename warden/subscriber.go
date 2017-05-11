package warden

import (
	"io"

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
	Oracle signedEncryptedShard
	Sign   SignedKeyPair
	Invite SignedKeyPair
}

func NewSubscriber(random io.Reader, pass []byte, fns ...func(*SubscriberOptions)) (Subscriber, signedEncryptedShard, error) {
	return Subscriber{}, signedEncryptedShard{}, nil
	/*  opts := buildSubscriberOptions(fns...) */
	//
	// secret, err := genSecret(random, opts.Oracle)
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	// defer secret.Destroy()
	//
	// secretKey, err := secret.Format()
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	// defer cryptoBytes(secretKey).Destroy()
	//
	// sign, err := opts.Sign.Algorithm.Gen(random, opts.Invite.Strength)
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	// defer sign.Destroy()
	//
	// invite, err := opts.Invite.Algorithm.Gen(random, opts.Invite.Strength)
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	// defer invite.Destroy()
	//
	// privShard, err := genEncryptedShard(random, secret, pass, opts.Oracle)
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	//
	// signKey, err := genKeyPair(random, sign, secretKey, opts.Invite)
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	//
	// inviteKey, err := genKeyPair(random, invite, secretKey, opts.Invite)
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	//
	// secretSig, err := secret.Sign(random, sign, opts.Sign.Hash)
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	//
	// secretKeySig, err := secretKey.Sign(random, sign, opts.Sign.Hash)
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	//
	// // TODO: this is self signed..don't think that matters
	// signedSignKey, err := signKey.Sign(random, sign, opts.Sign.Hash)
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	//
	// signedInviteKey, err := inviteKey.Sign(random, sign, opts.Sign.Hash)
	// if err != nil {
	// return Subscriber{}, signedEncryptedShard{}, errors.WithStack(err)
	// }
	//
	/* return Subscriber{uuid.NewV1(), secretSig, signedSignKey, signedInviteKey}, secretKeySig, nil */
}
