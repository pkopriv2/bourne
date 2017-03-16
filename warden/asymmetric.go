package warden

type ExchangeAlg int32

const (
	RSA ExchangeAlg = iota
)

type asymCipherText struct {
	Sig []byte
}
