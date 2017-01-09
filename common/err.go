package common

import (
	"fmt"
	"reflect"
	"time"
)

type TimeoutError struct {
	timeout time.Duration
	msg     string
}

func NewTimeoutError(timeout time.Duration, msg string) TimeoutError {
	return TimeoutError{timeout, msg}
}

func (t TimeoutError) Error() string {
	return fmt.Sprintf("Timeout[%v]: %v", t.timeout, t.msg)
}

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
