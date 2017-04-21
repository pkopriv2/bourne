package warden

import "io"

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
	Identity KeyPair
	Oracle   SignedOracle
}

func (s Subscriber) Id() string {
	return s.Identity.Pub.Id()
}

func NewSubscriber(rand io.Reader, priv PrivateKey, pass []byte, fns ...func(*SubscriberOptions)) (s Subscriber, a SignedOracleKey, e error) {
	opts := buildSubscriberOptions(fns...)

	oracle, line, err := genOracle(rand, opts.OracleOptions)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, err
	}

	oracleKey, err := genOracleKey(rand, line, pass, opts.OracleOptions)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, err
	}

	signedOracle, err := oracle.Sign(rand, priv, opts.SigningHash)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, err
	}

	signedOracleKey, err := oracleKey.Sign(rand, priv, opts.SigningHash)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, err
	}

	ident, err := genKeyPair(rand, priv, line.Bytes(), opts.SigningCipher, opts.SigningHash, opts.SigningSalt, opts.SigningIter)
	if err != nil {
		return Subscriber{}, SignedOracleKey{}, err
	}

	return Subscriber{ident, signedOracle}, signedOracleKey, nil
}
