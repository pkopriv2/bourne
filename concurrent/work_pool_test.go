package concurrent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPool_Close_Empty(t *testing.T) {
	pool := NewWorkPool(1)
	assert.Nil(t, pool.Close())
}

// func TestPool_Close_Single(t *testing.T) {
	// pool := NewWorkPool(1)
	// // ret := make(chan interface{})
	// // assert.Nil(t, pool.Close())
// }
