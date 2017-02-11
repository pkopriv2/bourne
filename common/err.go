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

func Extract(err error, match ... error) error {
	if err == nil {
		return nil
	}

	for _, cur := range match {
		if err == cur {
			return cur
		}
	}

	str := err.Error()
	for _, cur := range match {
		if str == cur.Error() {
			return cur
		}
	}

	// recurse now (more performant to do direct comaparisons first.)
	if cause := Extract(cause(err)); cause != nil {
		return cause
	}

	return err
}

func cause(err error) error {
	type causer interface {
		Cause() error
	}

	cause, ok := err.(causer)
	if !ok {
		return nil
	}

	return cause.Cause()
}
