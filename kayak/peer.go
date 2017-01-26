package kayak

import (
	"fmt"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

type roster struct {
	raw []peer
	ver *ref
}

func (c roster) Wait(next int) ([]peer, int, bool) {
	_, ok := c.ver.WaitForChange(next)
	peers, ver := c.Get()
	return peers, ver, ok
}

func (c roster) Notify() {
	c.ver.Notify()
}

func (c roster) Update(peers []peer) {
	c.ver.Update(func(cur int) int {
		c.raw = peers
		return cur + 1
	})
}

// not taking copy as it is assumed that array is immutable
func (c roster) Get() (peers []peer, ver int) {
	c.ver.Update(func(cur int) int {
		peers, ver = c.raw, cur
		return cur
	})
	return
}

func (c roster) Close() {
	c.ver.Close()
}

// replicated configuration
type peers []peer

func (p peers) Write(w scribe.Writer) {
	w.WriteMessage("peers", p)
}

func readPeers(r scribe.Reader) (p []peer, e error) {
	e = r.ParseMessages("peers", (*peers)(&p), peerParser)
	return
}

func parsePeers(bytes []byte, def []peer) (p []peer, e error) {
	if bytes == nil {
		return def, nil
	}

	msg, err := scribe.Parse(bytes)
	if err != nil {
		return def, err
	}

	return readPeers(msg)
}

// A peer contains the identifying info of a cluster member.
type peer struct {
	Id   uuid.UUID
	Addr string
}

func newPeer(addr string) peer {
	return peer{Id: uuid.NewV1(), Addr: addr}
}

func (p peer) String() string {
	return fmt.Sprintf("Peer(%v, %v)", p.Id.String()[:8], p.Addr)
}

func (p peer) NewPool(ctx common.Context) net.ConnectionPool {
	return net.NewConnectionPool("tcp", p.Addr, 10, 2*time.Second)
}

func (p peer) Client(ctx common.Context) (*client, error) {
	raw, err := net.NewTcpClient(ctx, ctx.Logger(), p.Addr)
	if err != nil {
		return nil, err
	}

	return &client{raw}, nil
}

func (p peer) Connect(ctx common.Context, cancel <-chan struct{}) (*client, error) {
	// exponential backoff up to 2^6 seconds.
	for timeout := 1 * time.Second; ; {
		ch := make(chan *client)
		go func() {
			cl, err := p.Client(ctx)
			if err == nil && cl != nil {
				ch <- cl
			}
		}()

		timer := time.NewTimer(timeout)
		select {
		case <-cancel:
			return nil, ClosedError
		case <-timer.C:

			// 64 seconds is maximum timeout
			if timeout < 2^6*time.Second {
				timeout *= 2
			}

			continue
		case cl := <-ch:
			return cl, nil
		}
	}
}

func (p peer) Write(w scribe.Writer) {
	w.WriteUUID("id", p.Id)
	w.WriteString("addr", p.Addr)
}

func peerParser(r scribe.Reader) (interface{}, error) {
	var p peer
	var e error
	e = common.Or(e, r.ReadUUID("id", &p.Id))
	e = common.Or(e, r.ReadString("addr", &p.Addr))
	return p, e
}
