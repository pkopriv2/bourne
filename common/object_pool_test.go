package common

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type closer struct {
	val int
}

func (c *closer) Close() error {
	return nil
}

var fn = func() (io.Closer, error) {
	return &closer{1}, nil
}

func TestObjectPool_Close(t *testing.T) {
	pool := NewObjectPool(NewEmptyContext(), "pool", fn, 10)
	assert.Nil(t, pool.Close())
}

func TestObjectPool_TakeTimeout_Timeout(t *testing.T) {
	pool := NewObjectPool(NewEmptyContext(), "pool", fn, 1)
	defer pool.Close()
	assert.NotNil(t, pool.TakeTimeout(100*time.Millisecond))
	assert.Nil(t, pool.TakeTimeout(100*time.Millisecond))
}

func TestObjectPool_TakeTimeout_Success(t *testing.T) {
	pool := NewObjectPool(NewEmptyContext(), "pool", fn, 1)
	defer pool.Close()
	assert.Equal(t, &closer{1}, pool.TakeTimeout(100*time.Millisecond))
}
