package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var fn = func() (interface{}, error) {
	return 1, nil
}

func TestObjectPool_Close(t *testing.T) {
	pool := NewObjectPool(NewEmptyContext(), "pool", fn, 10)
	assert.Nil(t, pool.Close())
}

func TestObjectPool_TakeTimeout_Timeout(t *testing.T) {
	pool := NewObjectPool(NewEmptyContext(), "pool", fn, 1)
	defer pool.Close()
	assert.Equal(t, 1, pool.TakeTimeout(100*time.Millisecond))
	assert.Nil(t, pool.TakeTimeout(100*time.Millisecond))
}

func TestObjectPool_TakeTimeout_Success(t *testing.T) {
	pool := NewObjectPool(NewEmptyContext(), "pool", fn, 1)
	defer pool.Close()
	assert.Equal(t, 1, pool.TakeTimeout(100*time.Millisecond))
}
