package warden

import "crypto"

type Curve interface {
	Bytes() []byte
	AsPublicKey() (crypto.PublicKey, error)
	AsPrivateKey() (crypto.PrivateKey, error)
}

type Point interface {
	Curve(...Point) (Curve, error)
	Bytes() []byte
}

type CipherText interface {
	Decrypt(key []byte) ([]byte, error)
}
