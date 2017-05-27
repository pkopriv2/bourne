package warden

import "github.com/pkg/errors"

// A key pad gives access to the various authentication methods and will
// be used during the registration process.
//
// Future: Accept alternative login methods (e.g. pins, passwords, multi-factor, etc...)

type KeyPad interface {
	BySignature(Signer, Hash) error
	ByPassword(user []byte, pass []byte) error
}

// A one time keypad.  Destroyed after first use.
type oneTimePad struct {
	Creds credential
}

func (c *oneTimePad) BySignature(s Signer, h Hash) error {
	c.Creds = &signCreds{s, h}
	return nil
}

func (c *oneTimePad) ByPassword(user []byte, pass []byte) error {
	preHash, err := cryptoBytes(pass).Hash(SHA256)
	if err != nil {
		return errors.Wrapf(err, "Error pre-hashing user password [%v]", string(user))
	}

	c.Creds = &passCreds{user, preHash}
	return nil
}

func enterCreds(fn func(KeyPad) error) (credential, error) {
	pad := &oneTimePad{}
	if err := fn(pad); err != nil {
		return nil, errors.WithStack(err)
	}
	return pad.Creds, nil
}
