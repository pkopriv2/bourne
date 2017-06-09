package krypto

type KeyExchangeOptions struct {
	Cipher Cipher
	Hash   Hash
}

func buildKeyExchangeOpts(fns ...func(*KeyExchangeOptions)) KeyExchangeOptions {
	ret := KeyExchangeOptions{AES_256_GCM, SHA256}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}
