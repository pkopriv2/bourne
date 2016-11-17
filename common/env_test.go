package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnv_CloseEmpty(t *testing.T) {
	env := NewEnv()
	assert.Nil(t, env.Close())
}

func TestEnv_CloseMulti(t *testing.T) {
	env := NewEnv()
	assert.Nil(t, env.Close())
	assert.NotNil(t, env.Close())
}

func TestEnv_Close(t *testing.T) {
	env := NewEnv()

	closed := make(chan struct{}, 2)
	env.OnClose(func() {
		closed <- struct{}{}
	})
	env.OnClose(func() {
		closed <- struct{}{}
	})

	assert.Nil(t, env.Close())

	select {
	default:
		assert.FailNow(t, "Fail")
	case <-closed:
	}

	select {
	default:
		assert.FailNow(t, "Fail")
	case <-closed:
	}
}
