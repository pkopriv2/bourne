package warden

import (
	"io"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type SubscriberOptions struct {
	Oracle OracleOptions
	Invite KeyPairOptions
	Sign   KeyPairOptions
}

func (s *SubscriberOptions) InviteOptions(fn func(*KeyPairOptions)) {
	s.Invite = buildKeyPairOptions(fn)
}

func buildSubscriberOptions(fns ...func(*SubscriberOptions)) SubscriberOptions {
	ret := SubscriberOptions{buildOracleOptions(), buildKeyPairOptions(), buildKeyPairOptions()}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

type Subscriber struct {
	Id     uuid.UUID
	Oracle SignedOracle
	Sign   SignedKeyPair
	Invite SignedKeyPair
}

func NewSubscriber(random io.Reader, pass []byte, fns ...func(*SubscriberOptions)) (Subscriber, SignedOracleKey, error) {
	opts := buildSubscriberOptions(fns...)

	oracle, line, err := genOracle(random, opts.Oracle)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}
	defer line.Destroy()

	sign, err := opts.Sign.Algorithm.Gen(random, opts.Invite.Strength)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}
	defer sign.Destroy()

	invite, err := opts.Invite.Algorithm.Gen(random, opts.Invite.Strength)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}
	defer invite.Destroy()

	oracleKey, err := genOracleKey(random, line, pass, opts.Oracle)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}

	signPair, err := genKeyPair(random, sign, line.Bytes(), opts.Invite)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}

	invPair, err := genKeyPair(random, invite, line.Bytes(), opts.Invite)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}

	signedOracle, err := oracle.Sign(random, sign, opts.Sign.Hash)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}

	signedOracleKey, err := oracleKey.Sign(random, sign, opts.Sign.Hash)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}

	// TODO: this is self signed..don't think that matters
	signedSignKey, err := signPair.Sign(random, sign, opts.Sign.Hash)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}

	signedInviteKey, err := invPair.Sign(random, sign, opts.Sign.Hash)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}

	return Subscriber{uuid.NewV1(), signedOracle, signedSignKey, signedInviteKey}, signedOracleKey, nil
}
