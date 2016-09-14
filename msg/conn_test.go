package msg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConnector_initError(t *testing.T) {
	factory := func() (Connection, error) {
		return nil, ErrConnectionFailure
	}

	conn := NewConnector(factory, 3, time.Second)
	assert.NotNil(t, conn)
}

func TestConnector_initTimeout(t *testing.T) {
	factory := func() (Connection, error) {
		time.Sleep(2 * time.Second)
		return nil, nil
	}

	conn := NewConnector(factory, 3, time.Second)
	assert.NotNil(t, conn)
}
