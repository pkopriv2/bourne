package errors

import "fmt"

func NewWrappedError(format string) func(reason error) error {
	return func(reason error) error {
		return fmt.Errorf(format, reason)
	}
}
