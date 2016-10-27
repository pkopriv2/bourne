package convoy

import (
	"errors"
	"testing"

	"github.com/pkopriv2/bourne/net"
)

var failure = errors.New("failure")

// var successHandler = func(Request) Response {
// return NewSuccessResponse(nil)
// }
//
// var failureHandler = func(Request) Response {
// return NewErrorResponse(failure)
// }
//
func TestMember_Conn(t *testing.T) {
	server, _ := net.NewTCPServer(0, func(req net.Request) net.Response {
		return net.NewSuccessResponse(nil)
	})
	defer server.Close()
}

//
// conn := conn.NewTCPConnectionFactory("localhost", 9000)
// member := newMember(uuid.NewV4(), net.NewMemConnectionFactory(), 0)
// conn, err := member.Conn()
// assert.NotNil(t, conn)
// assert.Nil(t, err)
// }
//
// func TestMember_Client(t *testing.T) {
// member := newMember(uuid.NewV4(), net.NewMemConnectionFactory(), 0)
// client, err := member.client()
// assert.NotNil(t, client)
// assert.Nil(t, err)
// }
//
// func TestMember_Client_Ping_Timeout(t *testing.T) {
// member := newMember(uuid.NewV4(), net.NewMemConnectionFactory(), 0)
// client, _ := member.client()
//
// success, err := client.Ping(10 * time.Millisecond)
// }
