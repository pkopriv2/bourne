package warden

type SigAlg int

type Signature struct {
	Alg int
	Raw []byte
}

func (s Signature) Validate(msg []byte) error {
	return nil
}
