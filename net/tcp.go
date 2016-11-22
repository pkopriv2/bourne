package net

import (
	"fmt"
	"net"

	"github.com/pkopriv2/bourne/scribe"
)

func ListenTcp(port string) (*TcpListener, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}

	return &TcpListener{listener: listener}, nil
}

func ConnectTcp(addr string) (*TcpConnection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &TcpConnection{conn}, nil
}

func ReadTcpConnectionFactory(r scribe.Reader) (ConnectionFactory, error) {
	var addr string
	if err := r.Read("addr", &addr); err != nil {
		return nil, err
	}

	return NewTcpConnectionFactory(addr), nil
}

func NewTcpConnectionFactory(addr string) ConnectionFactory {
	return &TcpConnectionFactory{addr}
}

type TcpConnectionFactory struct {
	addr string
}

func (t *TcpConnectionFactory) Write(w scribe.Writer) {
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

	return &TcpConnection{conn}, nil
}

type TcpConnection struct {
	conn net.Conn
}

func (u *TcpConnection) Close() error {
	return u.conn.Close()
}

func (t *TcpConnection) Read(p []byte) (n int, err error) {
	return t.conn.Read(p)
}

func (t *TcpConnection) Write(p []byte) (n int, err error) {
	return t.conn.Write(p)
}

func (t *TcpConnection) Factory() ConnectionFactory {
	return NewTcpConnectionFactory(t.conn.RemoteAddr().String())
}
