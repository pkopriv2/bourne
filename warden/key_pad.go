package warden

import "github.com/pkg/errors"

// A key pad gives access to the various authentication methods and will
// be used during the registration process.
//
// Future: Accept alternative login methods (e.g. pins, passwords, multi-factor, etc...)

type KeyPad interface {
	BySignature(Signer, ...func(*SessionOptions)) error
}

// A one time keypad.  Destroyed after first use.
type oneTimePad struct {
	Opts   SessionOptions
	Signer Signer
}

func (c *oneTimePad) BySignature(s Signer, opts ...func(*SessionOptions)) error {
	c.Opts = buildSessionOptions(opts...)
	c.Signer = s
	return nil
}

func enterCreds(fn func(KeyPad) error) (*oneTimePad, error) {
	pad := &oneTimePad{}
	if err := fn(pad); err != nil {
		return nil, errors.WithStack(err)
	}
	return pad, nil
}
