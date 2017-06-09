package secret

import "crypto"

// Options for generating a shared secret.
type SecretOptions struct {
	Algorithm SecretAlgorithm
	Entropy   int
	Hash      crypto.Hash
}

func defaultSecretOptions() SecretOptions {
	return SecretOptions{ShamirAlpha, 32, crypto.SHA256}
}

func buildSecretOptions(fns ...func(*SecretOptions)) SecretOptions {
	ret := defaultSecretOptions()
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}
