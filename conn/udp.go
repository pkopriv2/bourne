package conn

import (
	"fmt"
	"net"
)

func NewUDPConnectionFactory(host string, port int) ConnectionFactory {
	return &UDPConnectionFactory{host, port}
}

func ListenUDP(port int) (Listener, error) {
	conn, err := net.Listen("udp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}

	return &UDPListener{conn}, nil
}

func ConnectUDP(host string, port int) (Connection, error) {
	conn, err := net.Dial("udp", fmt.Sprintf("%v:%v", host, port))
	if err != nil {
		return nil, err
	}

	return &UDPConnection{conn}, nil
}

type UDPConnectionFactory struct {
	host string
	port int
}

func (u *UDPConnectionFactory) Conn() (Connection, error) {
	return ConnectUDP(u.host, u.port)
}

type UDPListener struct {
	listener net.Listener
}

func (u *UDPListener) Accept() (Connection, error) {
	conn, err := u.listener.Accept()
	if err != nil {
		return nil, err
	}

	return &UDPConnection{conn}, nil
}

type UDPConnection struct {
	conn net.Conn
}

func (u *UDPConnection) Read(p []byte) (n int, err error) {
	return u.conn.Read(p)
}

func (u *UDPConnection) Write(p []byte) (n int, err error) {
	return u.conn.Write(p)
}

func (u *UDPConnection) Close() error {
	return u.conn.Close()
}
