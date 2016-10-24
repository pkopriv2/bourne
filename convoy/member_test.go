package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestMember_Conn(t *testing.T) {
	member := newMember(uuid.NewV4(), net.NewMemConnectionFactory(), 0)
	conn, err := member.Conn()
	assert.NotNil(t, conn)
	assert.Nil(t, err)
}

func TestMember_Client(t *testing.T) {
	member := newMember(uuid.NewV4(), net.NewMemConnectionFactory(), 0)
	client, err := member.client()
	assert.NotNil(t, client)
	assert.Nil(t, err)
}

func TestMember_Client_Ping_Timeout(t *testing.T) {
	member := newMember(uuid.NewV4(), net.NewMemConnectionFactory(), 0)
	client, _ := member.client()

	success, err := client.Ping(10 * time.Millisecond)
}
