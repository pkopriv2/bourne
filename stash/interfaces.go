package stash

import "github.com/pkopriv2/bourne/common"

type Update func(string) interface{}

type Stash interface {
	Get(string) io.Reader
	Apply(string, Update)
}

func NewStash(context common.Context, path string) Stash {
	return nil
}
