package net

import (
	"fmt"
	"net"
	"strings"

	"github.com/pkopriv2/bourne/concurrent"
)

func NewTCPConnectionFactory(addr string) ConnectionFactory {
	return &TCPConnectionFactory{addr}
}

func ListenTCP(port int) (Listener, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}

	return &TCPListener{listener: listener}, nil
}

func ConnectTCP(addr string) (Connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func LocalAddress(conn net.Conn) string {
	localAddr := conn.LocalAddr().String()
	idx := strings.LastIndex(localAddr, ":")
	return localAddr[0:idx]
}

type TCPConnectionFactory struct {
	addr string
}

func (u *TCPConnectionFactory) Conn() (Connection, error) {
	return ConnectTCP(u.addr)
}

type TCPListener struct {
	listener net.Listener
	closed   concurrent.AtomicBool
}

func (u *TCPListener) Close() error {
	if !u.closed.Swap(false, true) {
		return ListenerClosedError
	}

	return u.listener.Close()
}

func (u *TCPListener) Conn() (Connection, error) {
	if !u.closed.Get() {
		return nil, ListenerClosedError
	}

	return ConnectTCP(u.listener.Addr().String())
}

func (u *TCPListener) Accept() (Connection, error) {
	conn, err := u.listener.Accept()
	if err != nil {
		return nil, err
	}

	return conn, nil
}
