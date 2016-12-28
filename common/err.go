package common

import "reflect"

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
