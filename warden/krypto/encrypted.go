package krypto

// A decrypter decrypts itself with the given key
type Encrypted interface {
	Decrypt(key []byte) ([]byte, error)
}

