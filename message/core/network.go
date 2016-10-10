package core

import (
	"github.com/pkopriv2/bourne/circuit"
	"github.com/pkopriv2/bourne/common"
)

const (
	confNetworkTxSize       = "bourne.msg.mux.tx.size"
	confNetworkRxSize       = "bourne.msg.mux.rx.size"
	confNetworkSocketRxSize = "bourne.msg.mux.socket.size"
	confNetworkNumRouters   = "bourne.msg.mux.num.routers"
)

const (
	defaultNetworkTxSize       = 4096
	defaultNetworkRxSize       = 4096
	defaultNetworkSocketRxSize = 1024
	defaultNetworkNumRouters   = 3
)

type Network interface {
	circuit.Controller
	Context() common.Context
	AddRoute(addr interface{}) StandardSocket
}

// type networkSocket struct {
// net  *network
// rx   chan wire.Packet
// tx   chan wire.Packet
// done chan struct{}
// fail chan struct{}
// }
//
// func newNetworkSocket(net *network) *networkSocket {
// rx := make(chan wire.Packet)
// tx := make(chan wire.Packet)
// return &networkSocket{net, rx, tx}
// }
//
// func (s *networkSocket) Done() {
// close(s.done)
// }
//
// func (s *networkSocket) Fail(e error) {
// }
//
// func (s *networkSocket) Closed() <-chan struct{} {
// return s.net.close
// }
//
// func (s *networkSocket) Failed() <-chan struct{} {
// return s.net.failed
// }
//
// func (s *networkSocket) Failure() error {
// return s.net.failure // todo make
// }
//
// func (s *networkSocket) Rx() <-chan wire.Packet {
// return s.rx
// }
//
// func (s *networkSocket) Tx() chan<- wire.Packet {
// return s.tx
// }
//
// type network struct {
// ctx common.Context
//
// failed chan struct{}
// closed chan struct{}
// fail   chan error
// close  chan struct{}
// failure error
//
// router Router
// sockets concurrent.Map
// routers concurrent.List // List[Router]
//
// wait *wait
// }
//
// func NewNetwork(ctx common.Context, r Router) Network {
// ret := &network{
// ctx: ctx}
//
// // start the control router
// ret.wait.Add()
// go func() {
// ret.wait.Done()
//
// select {
// case e := <-ret.fail:
// ret.failure = e
// close(ret.failed)
// case <-ret.close:
// close(ret.closed)
// }
// }()
//
// return ret
// }
//
// func (n *network) Close() {
// select {
// case n.close <- struct{}{}:
// case <-n.closed:
// case <-n.failed:
// }
// }
//
// func (n *network) Fail(e error) {
// select {
// case n.fail <- e:
// case <-n.closed:
// case <-n.failed:
// }
// }
//
// func (n *network) Wait() <-chan struct{} {
// return n.wait.Wait()
// }
//
// func (n *network) Context() common.Context {
// return n.ctx
// }
//
// func (n *network) AddRoute(addr interface{}) StandardSocket {
// socket := newNetworkSocket(n)
//
// n.wait.Add()
// go Route(n, socket, n.router)
//
// n.sockets.Put(addr, socket)
// return socket
// }
//
// func Route(n *network, s *networkSocket, fn Router) {
// defer n.wait.Done()
//
// logger := n.Context().Logger()
// for {
// var p wire.Packet
// select {
// case <-n.failed:
// return
// case <-n.closed:
// return
// case <-s.done:
// n.sockets.Remove(s)
// return
// case p = <-s.tx:
// }
//
// addr := fn(p)
// if addr == nil {
// logger.Info("Dropping packet: %v", p)
// continue
// }
//
// dst := n.sockets.Get(addr).(*networkSocket)
// if dst == nil {
// logger.Info("Destination does not exist: %v", p)
// continue
// }
//
// select {
// case <-n.failed:
// return
// case <-n.closed:
// return
// case <-s.done:
// n.sockets.Remove(s)
// return
// case dst.rx <- p:
// }
// }
// }
