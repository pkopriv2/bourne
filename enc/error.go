package enc

import "errors"

var empty string

func ParseError(msg string) error {
	if msg == empty {
		return nil
	}

	return errors.New(msg)
}
