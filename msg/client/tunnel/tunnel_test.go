package tunnel

// import (
	// "testing"
	// "time"
//
	// "github.com/pkopriv2/bourne/msg/wire"
	// uuid "github.com/satori/go.uuid"
	// "github.com/stretchr/testify/assert"
// )
//
// func TestActiveTunnel_openInitTimeout(t *testing.T) {
	// _, tunnel := newTestTunnel(uuid.NewV4(), 0, uuid.NewV4(), 1, false)
	// defer tunnel.Close()
//
	// assert.Equal(t, TunnelFailure, tunnel.state.Get())
// }

//
// func TestActiveTunnel_openRecvTimeout(t *testing.T) {
// _, tunnel := newTestTunnel(uuid.NewV4(), 0, uuid.NewV4(), 1, true)
// defer tunnel.Close()
//
// // start the sequence, but never respond
// tunnel.send(wire.BuildPacket(tunnel.Route().Reverse()).SetOpen(1).Build())
// tunnel.state.WaitUntil(TunnelOpened | TunnelFailure)
//
// assert.Equal(t, TunnelFailure, tunnel.state.Get())
// }
//
// func TestActiveTunnel_openHandshake(t *testing.T) {
// tunnelL, tunnelR := newTestTunnelPair(uuid.NewV4(), 0, uuid.NewV4(), 1)
// defer tunnelL.Close()
// defer tunnelR.Close()
//
// tunnelL.state.WaitUntil(TunnelOpened | TunnelClosed | TunnelFailure)
// tunnelR.state.WaitUntil(TunnelOpened | TunnelClosed | TunnelFailure)
//
// assert.Equal(t, TunnelOpened, tunnelL.state.Get())
// assert.Equal(t, TunnelOpened, tunnelR.state.Get())
// }
//
//
// func TestActiveTunnel_sendSinglePacket(t *testing.T) {
// tunnelL, tunnelR := newTestTunnelPair(uuid.NewV4(), 0, uuid.NewV4(), 1)
// defer tunnelR.Close()
// defer tunnelL.Close()
//
// tunnelR.Write([]byte{1})
//
// buf := make([]byte, 1024)
//
// num, err := tunnelL.Read(buf)
// if err != nil {
// t.Fail()
// }
//
// assert.Equal(t, 1, num)
// assert.Equal(t, []byte{1}, buf[:1])
// }
//
// func TestActiveTunnel_sendSingleStream(t *testing.T) {
// tunnelL, tunnelR := newTestTunnelPair(uuid.NewV4(), 0, uuid.NewV4(), 1)
// tunnelR.debug = false
// defer tunnelR.Close()
// defer tunnelL.Close()
//
// go func() {
// for i := 0; i < 255; i++ {
// if _, err := tunnelL.Write([]byte{uint8(i)}); err != nil {
// tunnelL.log("Error writing to tunnel: %v", err)
// return
// }
// }
// }()
//
// timer := time.NewTimer(time.Second * 10)
//
// buf := make([]byte, 1024)
// tot := 0
// for {
// select {
// case <-timer.C:
// t.Fail()
// return
// default:
// break
// }
//
// num, err := tunnelR.Read(buf[tot:])
// if err != nil {
// t.Fail()
// break
// }
//
// tot += num
// if tot >= 255 {
// break
// }
// }
//
// assert.Equal(t, 255, tot)
// for i := 0; i < 255; i++ {
// assert.Equal(t, uint8(i), buf[i])
// }
// }
//
// func TestActiveTunnel_sendDuplexStream(t *testing.T) {
// tunnelL, tunnelR := newTestTunnelPair(uuid.NewV4(), 0, uuid.NewV4(), 1)
// defer tunnelR.Close()
// defer tunnelL.Close()
//
// go func() {
// for i := 0; i < 255; i++ {
// tunnelL.Write([]byte{uint8(i)})
// }
// }()
//
// go func() {
// for i := 0; i < 255; i++ {
// tunnelR.Write([]byte{uint8(i)})
// }
// }()
//
// timer := time.NewTimer(time.Second * 10)
//
// bufR := make([]byte, 1024)
// totR := 0
// for {
// select {
// case <-timer.C:
// t.Fail()
// return
// default:
// break
// }
//
// num, err := tunnelR.Read(bufR[totR:])
// if err != nil {
// t.Fail()
// break
// }
//
// totR += num
// if totR >= 255 {
// break
// }
// }
//
// assert.Equal(t, 255, totR)
// for i := 0; i < 255; i++ {
// assert.Equal(t, uint8(i), bufR[i])
// }
//
// bufL := make([]byte, 1024)
// totL := 0
// for {
// select {
// case <-timer.C:
// t.Fail()
// return
// default:
// break
// }
//
// num, err := tunnelL.Read(bufL[totL:])
// if err != nil {
// t.Fail()
// break
// }
//
// totL += num
// if totL >= 255 {
// break
// }
// }
//
// assert.Equal(t, 255, totL)
// for i := 0; i < 255; i++ {
// assert.Equal(t, uint8(i), bufL[i])
// }
// }
//
// func TestActiveTunnel_sendLargeStream(t *testing.T) {
// tunnelL, tunnelR := newTestTunnelPair(uuid.NewV4(), 0, uuid.NewV4(), 1)
// defer tunnelR.Close()
// defer tunnelL.Close()
//
// out := make([]byte, 1024)
// for i := 0; i < 1024; i++ {
// out[i] = byte(i)
// }
//
// go func() {
// for i := 0; i < 1<<10; i++ {
// tunnelL.Write(out)
// }
// }()
//
// timer := time.NewTimer(time.Second * 45)
//
// buf := make([]byte, 1<<14)
// tot := 0
// for {
// select {
// case <-timer.C:
// t.Fail()
// return
// default:
// break
// }
//
// num, err := tunnelR.Read(buf)
// if err != nil {
// t.Fail()
// break
// }
//
// tot += num
// if tot >= 1<<20 {
// break
// }
// }
//
// assert.Equal(t, 1<<20, tot)
// }
//
// func newTestTunnel(memberIdL uuid.UUID, tunnelIdL uint64, memberIdR uuid.UUID, tunnelIdR uint64, listener bool) (chan wire.Packet, *tunnel) {
	// l := wire.NewAddress(memberIdL, tunnelIdL)
	// r := wire.NewAddress(memberIdR, tunnelIdR)
//
	// out := make(chan wire.Packet, 1<<10)
//
	// tunnel := newTunnel(wire.NewRemoteRoute(l, r), listener, func(opts *TunnelOptions) {
		// opts.OnClose = func(c Tunnel) error {
			// close(out)
			// return nil
		// }
		// opts.OnData = func(p wire.Packet) error {
			// out <- p
			// return nil
		// }
	// })
//
	// tunnel.config.debug = true
	// tunnel.config.ackTimeout = 500 * time.Millisecond
	// tunnel.config.closeTimeout = 500 * time.Millisecond
	// tunnel.config.sendWait = 1 * time.Millisecond
	// tunnel.config.recvWait = 1 * time.Millisecond
//
	// return out, tunnel
// }
//
// func newTestRouter() func(outL chan wire.Packet, outR chan wire.Packet, tunnelL *tunnel, tunnelR *tunnel) {
	// return func(outL chan wire.Packet, outR chan wire.Packet, tunnelL *tunnel, tunnelR *tunnel) {
		// for {
			// select {
			// case p, ok := <-outR:
				// if !ok {
					// return
				// }
				// tunnelL.log("Routing packet: %v", p)
				// if err := tunnelL.send(p); err != nil {
					// return
				// }
			// case p, ok := <-outL:
				// if !ok {
					// return
				// }
				// tunnelR.log("Routing packet: %v", p)
				// if err := tunnelR.send(p); err != nil {
					// return
				// }
			// }
		// }
	// }
// }
//
// func newTestTunnelPairWithRouter(memberIdL uuid.UUID, tunnelIdL uint64, memberIdR uuid.UUID, tunnelIdR uint64, router func(chan wire.Packet, chan wire.Packet, *tunnel, *tunnel)) (*tunnel, *tunnel) {
	// outL, tunnelL := newTestTunnel(memberIdL, tunnelIdL, memberIdR, tunnelIdR, false)
	// outR, tunnelR := newTestTunnel(memberIdR, tunnelIdR, memberIdL, tunnelIdL, true)
//
	// go router(outL, outR, tunnelL, tunnelR)
//
	// return tunnelL, tunnelR
// }
//
// func newTestTunnelPair(memberIdL uuid.UUID, tunnelIdL uint64, memberIdR uuid.UUID, tunnelIdR uint64) (*tunnel, *tunnel) {
	// return newTestTunnelPairWithRouter(memberIdL, tunnelIdL, memberIdR, tunnelIdR, newTestRouter())
// }
