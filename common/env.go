package common

import "github.com/pkopriv2/bourne/concurrent"

// TODO: This is pretty Pointless....
type Env interface {
	Data() concurrent.Map
}

type env struct {
	data concurrent.Map
}

func NewEnv() *env {
	return &env{data: concurrent.NewMap()}
}

func (e *env) Data() concurrent.Map {
	return e.data
}
