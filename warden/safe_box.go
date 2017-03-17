package warden

import (
	"crypto"

	uuid "github.com/satori/go.uuid"
)

// A safe box is a local only storage
type accessCode struct {
	point aSymCipherText
}

func (a accessCode) DecryptPoint(key crypto.PrivateKey) (point, error) {
	// raw, err := a.point.Decrypt(key)
	// if err != nil {
	// return point{}, errors.WithStack(err)
	// }
	panic("not implemented")
}

type safe struct {
	id       uuid.UUID
	pubPoint point
	pubKey   crypto.PublicKey
	// codes    map[string]
}

func (s *safe) Id() uuid.UUID {
	panic("not implemented")
}

func (s *safe) PublicKey() crypto.PublicKey {
	panic("not implemented")
}

func (s *safe) AccessCodes() []AccessCodeInfo {
	panic("not implemented")
}

func (s *safe) Open(challengeName string, challengeSecret []byte, fn func(s SafeContents)) (err error) {
	panic("not implemented")
}
