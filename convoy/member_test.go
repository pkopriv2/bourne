package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/enc"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestMember_WriteRead(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	member := newMember(ctx, uuid.NewV4(), net.NewTcpConnectionFactory("localhost:8000"), 1)

	actual, err := readMember(ctx, enc.Write(member))
	assert.Nil(t, err)

	assert.Equal(t, member, actual)
}

func TestClient_Ping(t *testing.T) {
	called := concurrent.NewAtomicBool()
	server, _ := net.NewTcpServer(common.NewContext(common.NewEmptyConfig()), 0, func(req net.Request) net.Response {
		called.Set(true)
		return net.NewEmptyResponse()
	})
	defer server.Close()

	rawClient, err := server.Client()
	assert.Nil(t, err)

	client := newClient(rawClient)
	defer client.Close()
	success, err := client.Ping()

	assert.Nil(t, err)
	assert.True(t, called.Get())
	assert.True(t, success)
}

func TestClient_PingProxy(t *testing.T) {
	id := uuid.NewV4()
	server, _ := net.NewTcpServer(common.NewContext(common.NewEmptyConfig()), 0, func(req net.Request) net.Response {
		actual, err := readPingProxyRequest(req)
		assert.Nil(t, err)
		assert.Equal(t, id, actual)
		return newPingProxyResponse(true)
	})
	defer server.Close()

	rawClient, err := server.Client()
	assert.Nil(t, err)

	client := newClient(rawClient)
	defer client.Close()
	success, err := client.PingProxy(id)

	assert.Nil(t, err)
	assert.True(t, success)
}

func TestClient_Update(t *testing.T) {
	updates := []update{newLeave(uuid.NewV4(), 0), newLeave(uuid.NewV4(), 0)}

	server, _ := net.NewTcpServer(common.NewContext(common.NewEmptyConfig()), 0, func(req net.Request) net.Response {
		actual, err := readUpdateRequest(common.NewContext(common.NewEmptyConfig()), req)
		assert.Nil(t, err)
		assert.Equal(t, updates, actual)

		success := make([]bool, 0, len(updates))
		for range updates {
			success = append(success, true)
		}

		return newUpdateResponse(success)
	})
	defer server.Close()

	rawClient, err := server.Client()
	assert.Nil(t, err)

	client := newClient(rawClient)
	defer client.Close()
	success, err := client.Update(updates)

	assert.Nil(t, err)
	assert.Equal(t, []bool{true, true}, success)
}
