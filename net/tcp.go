package net

import (
	"net"
	"time"

	"github.com/pkg/errors"
)

func NewTcpNetwork() *TcpNetwork {
	return &TcpNetwork{}
}

func ListenTcp(timeout time.Duration, addr string) (Listener, error) {
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		return nil, err
	}

	return &TcpListener{listener: listener, timeout: timeout}, nil
}

func ConnectTcp(timeout time.Duration, addr string) (Connection, error) {
	conn, err := net.DialTimeout("tcp4", addr, timeout)
	if err != nil {
		return nil, err
	}

	if conn == nil {
		return nil, errors.Errorf("Error opening connection [%v]", addr)
	}

	return &TcpConnection{conn, timeout}, nil
}

type TcpNetwork struct {
}

func (t *TcpNetwork) Dial(timeout time.Duration, addr string) (Connection, error) {
	return ConnectTcp(timeout, addr)
}

func (t *TcpNetwork) Listen(timeout time.Duration, addr string) (Listener, error) {
	return ListenTcp(timeout, addr)
}

type TcpListener struct {
	listener net.Listener
	timeout  time.Duration
}

func (u *TcpListener) Close() error {
	return u.listener.Close()
}

func (u *TcpListener) Addr() Addr {
	return u.listener.Addr()
}

func (u *TcpListener) Conn() (Connection, error) {
	return ConnectTcp(u.timeout, u.listener.Addr().String())
}

func (u *TcpListener) Accept() (Connection, error) {
	conn, err := u.listener.Accept()
	if err != nil {
		return nil, err
	}

	return &TcpConnection{conn, u.timeout}, nil
}

type TcpConnection struct {
	conn    net.Conn
	timeout time.Duration
}

func (u *TcpConnection) Close() error {
	return u.conn.Close()
}

func (t *TcpConnection) Read(p []byte) (n int, err error) {
	t.conn.SetReadDeadline(time.Now().Add(t.timeout))
	return t.conn.Read(p)
}

func (t *TcpConnection) Write(p []byte) (n int, err error) {
	t.conn.SetWriteDeadline(time.Now().Add(t.timeout))
	return t.conn.Write(p)
}

func (t *TcpConnection) Local() net.Addr {
	return t.conn.LocalAddr()
}

func (t *TcpConnection) Remote() net.Addr {
	return t.conn.RemoteAddr()
}
