package warden

import (
	"io"

	"github.com/pkg/errors"
)

// A safe box is a local only storage
// FIXME: THIS IS A PROTOTYPE IMPL OF THE SAFE

// Generates a encrypted access code
func genAccessCode(cipher SymCipher, rand io.Reader, salt []byte, iter int, line line, code Bytes) (accessCode, error) {
	point, err := randomPoint(rand, line, cipher.KeySize()*2)
	if err != nil {
		return accessCode{}, errors.WithStack(err)
	}
	defer point.Destroy()

	key := code.PBKDF2(salt, iter, cipher.KeySize())
	defer key.Destroy()

	encrypted, err := symmetricEncrypt(rand, cipher, key, point.Bytes())
	if err != nil {
		return accessCode{}, errors.WithStack(err)
	}

	return accessCode{encrypted}, nil
}

//
type accessCode struct {
	point symCipherText
	// created  time.Time
	// accessed time.Time
}

func (a accessCode) String() string {
	return a.point.String()
}

func (a accessCode) decryptPoint(salt []byte, code Bytes) (point, error) {
	raw, err := a.point.Decrypt(code.PBKDF2(salt, 4096, a.point.Cipher.KeySize()))
	if err != nil {
		return point{}, errors.WithStack(err)
	}

	return parsePointBytes(raw)
}

func (a accessCode) deriveLine(salt []byte, code []byte, pub point) (line, error) {
	priv, err := a.decryptPoint(salt, code)
	if err != nil {
		return line{}, errors.WithStack(err)
	}

	return priv.Derive(pub)
}

type safeContents struct {
	secret       Bytes
	secretLine   line
	secretCipher SymCipher
	codes        map[string]accessCode
	codeSalt     []byte
	codeIter     int
	rand         io.Reader
}

func (s *safeContents) Destroy() {
	s.secret.Destroy()
	s.secretLine.Destroy()
}

func (s *safeContents) Secret() []byte {
	return s.secret
}

func (s *safeContents) AccessCodes() []AccessCodeInfo {
	ret := make([]AccessCodeInfo, 0, len(s.codes))
	// for _, cur := range Acc
	return ret
}

func (s *safeContents) AddAccessCode(name string, code []byte, ttl int) error {
	gen, err := genAccessCode(s.secretCipher, s.rand, s.codeSalt, s.codeIter, s.secretLine, code)
	if err != nil {
		return errors.WithStack(err)
	}

	s.codes[name] = gen
	return nil
}

func (s *safeContents) DelAccessCode(name string) error {
	delete(s.codes, name)
	return nil
}

//
// type safe struct {
// id       uuid.UUID
// pubPoint point
// pubKey   crypto.PublicKey
// codes    map[string]accessCode
// secret   symCipherText
// }
//
// func (s *safe) Id() uuid.UUID {
// return s.id
// }
//
// func (s *safe) PublicKey() crypto.PublicKey {
// return s.pubKey
// }
//
// func (s *safe) AccessCodes() []AccessCodeInfo {
// panic("not implemented")
// }
//
// func (s *safe) Open(challengeName string, challengeSecret []byte, fn func(s SafeContents)) (err error) {
// panic("not implemented")
// }
