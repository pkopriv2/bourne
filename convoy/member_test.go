package convoy

import "errors"

var failure = errors.New("failure")

// var successHandler = func(Request) Response {
	// return NewSuccessResponse(nil)
// }
//
// var failureHandler = func(Request) Response {
	// return NewErrorResponse(failure)
// }
//
// func TestMember_Conn(t *testing.T) {
// server, _ := net.NewTCPServer(0, func(req Request) Response {
// return NewSuccessResponse(nil)
// })
//
// server, _ := conn.ListenTCP(9000)
// defer server.Close()
//
// go func() {
// conn := server.Accept()
// }
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
