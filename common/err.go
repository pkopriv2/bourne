package common

import (
	"errors"
	"reflect"
)

var (
	ClosedError    = errors.New("Common:ClosedError")
	TimeoutError   = errors.New("Common:TimeoutError")
	ExpiredError   = errors.New("Common:ExpiredError")
	CanceledError  = errors.New("Common:CanceledError")
)

// FIXME: Remove this! this is broken
func RunIf(fn func()) func(v interface{}) {
	return func(v interface{}) {
		val := reflect.ValueOf(v)
		if val.Kind() == reflect.Ptr && !val.IsNil() {
			fn()
		}
	}
}

func Or(l error, r error) error {
	if l != nil {
		return l
	} else {
		return r
	}
}
