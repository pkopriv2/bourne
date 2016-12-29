package net

import (
	"fmt"
	"net"

	"github.com/pkg/errors"
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

	if conn == nil {
		return nil, errors.Errorf("Error opening connection [%v]", addr)
	}

	return &TcpConnection{conn}, nil
}

type TcpListener struct {
	listener net.Listener
}

func (u *TcpListener) Close() error {
	return u.listener.Close()
}

func (u *TcpListener) Addr() net.Addr {
	return u.listener.Addr()
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

func (t *TcpConnection) LocalAddr() net.Addr {
	return t.conn.LocalAddr()
}

func (t *TcpConnection) RemoteAddr() net.Addr {
	return t.conn.RemoteAddr()
}
