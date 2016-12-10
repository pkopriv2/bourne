package common

func RunIf(fn func()) func(v interface{}) {
	return func(v interface{}) {
		if v != nil {
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
