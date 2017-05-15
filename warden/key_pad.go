package warden

import "github.com/pkg/errors"

// A key pad gives access to the various authentication methods and will
// be used during the registration process.
//
// Future: Accept alternative login methods (e.g. pins, passwords, multi-factor, etc...)

type KeyPad interface {
	WithSignature(Signer)
	// WithPassword(account string, pass []byte) error
}

// A one time keypad.  Destroyed after first use.
type oneTimePad struct {
	Signer Signer
}

func (c *oneTimePad) WithSignature(s Signer) {
	c.Signer = s
}

func enterCreds(fn func(KeyPad) error) (*oneTimePad, error) {
	pad := &oneTimePad{}

	if err := fn(pad); err != nil {
		return nil, errors.WithStack(err)
	}

	return pad, nil
}
