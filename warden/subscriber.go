package warden

import (
	"io"
	"math/rand"

	"github.com/pkg/errors"
)

type SubscriberOptions struct {
	OracleOptions

	SigningAlgorithm KeyAlgorithm
	SigningStrength  int
	SigningCipher    SymmetricCipher
	SigningHash      Hash
	SigningIter      int
	SigningSalt      int
}

func buildSubscriberOptions(fns ...func(*SubscriberOptions)) SubscriberOptions {
	ret := SubscriberOptions{defaultOracleOptions(), RSA, 2048, AES_256_GCM, SHA256, 1024, 32}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

type Subscriber struct {
	Pub    PublicKey
	Oracle SignedOracle
}

func NewSubscriber(random io.Reader, sign Signer, fns ...func(*SubscriberOptions)) (Subscriber, SignedOracleKey, error) {
	opts := buildSubscriberOptions(fns...)

	oracle, line, err := genOracle(random, opts.OracleOptions)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}
	defer line.Destroy()

	// FIXME: Determine if it's safe to use a signature like this. (ie as an encryption key)
	pass, err := sign.Sign(rand.New(rand.NewSource(0)), SHA256, sign.Public().Bytes())
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}
	defer Bytes(pass.Data).Destroy()

	oracleKey, err := genOracleKey(random, line, pass.Data, opts.OracleOptions)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}

	signedOracle, err := oracle.Sign(random, sign, opts.SigningHash)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}

	signedOracleKey, err := oracleKey.Sign(random, sign, opts.SigningHash)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, errors.WithStack(err)
	}

	return Subscriber{sign.Public(), signedOracle}, signedOracleKey, nil
}
