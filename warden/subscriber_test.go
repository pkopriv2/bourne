package warden

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubscriber(t *testing.T) {
	sub, key, e := NewSubscriber(rand.Reader, []byte("pass"))
	assert.Nil(t, e)

	line, e := sub.Oracle.Unlock(key.OracleKey, []byte("pass"))
	assert.Nil(t, e)

	_, e = sub.Sign.Decrypt(line.Bytes())
	assert.Nil(t, e)

	_, e = sub.Invite.Decrypt(line.Bytes())
	assert.Nil(t, e)
}
