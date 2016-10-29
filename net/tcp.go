package net

import (
	"fmt"
	"net"
	"strings"

	"github.com/pkopriv2/bourne/enc"
)

func ListenTcp(port int) (Listener, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}

	return &TcpListener{listener: listener}, nil
}

func ConnectTcp(addr string) (Connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func ReadTcpConnectionFactory(r enc.Reader) (ConnectionFactory, error) {
	var addr string
	if err := r.Read("addr", &addr); err != nil {
		return nil, err
	}

	return NewTcpConnectionFactory(addr), nil
}

func NewTcpConnectionFactory(addr string) ConnectionFactory {
	return &TcpConnectionFactory{addr}
}

func LocalAddress(conn net.Conn) string {
	localAddr := conn.LocalAddr().String()
	idx := strings.LastIndex(localAddr, ":")
	return localAddr[0:idx]
}

type TcpConnectionFactory struct {
	addr string
}

func (t *TcpConnectionFactory) Write(w enc.Writer) {
	w.Write("type", "tcp")
	w.Write("addr", t.addr)
}

func (u *TcpConnectionFactory) Conn() (Connection, error) {
	return ConnectTcp(u.addr)
}

type TcpListener struct {
	listener net.Listener
}

func (u *TcpListener) Close() error {
	return u.listener.Close()
}

func (u *TcpListener) Conn() (Connection, error) {
	return ConnectTcp(u.listener.Addr().String())
}

func (u *TcpListener) Accept() (Connection, error) {
	conn, err := u.listener.Accept()
	if err != nil {
		return nil, err
	}

	return conn, nil
}
