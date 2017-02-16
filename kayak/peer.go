package kayak

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

func hasPeer(peers []Peer, p Peer) bool {
	for _, cur := range peers {
		if cur.Id == p.Id {
			return true
		}
	}
	return false
}

func findPeer(peers []Peer, p Peer) int {
	for i, cur := range peers {
		if cur.Id == p.Id {
			return i
		}
	}
	return -1
}

func addPeer(cur []Peer, p Peer) []Peer {
	if hasPeer(cur, p) {
		return cur
	}

	return append(cur, p)
}

func delPeer(cur []Peer, p Peer) []Peer {
	index := findPeer(cur, p)
	if index == -1 {
		return cur
	}

	return append(cur[:index], cur[index+1:]...)
}

func equalPeers(l []Peer, r []Peer) bool {
	if len(l) != len(r) {
		return false
	}

	for i, p := range l {
		if p != r[i] {
			return false
		}
	}

	return true
}

func clusterBytes(cluster peers) []byte {
	return cluster.Bytes()
}

// replicated configuration
type peers []Peer

func (p peers) Write(w scribe.Writer) {
	w.WriteMessages("peers", p)
}

func (p peers) Bytes() []byte {
	return scribe.Write(p).Bytes()
}

func readPeers(r scribe.Reader) (p []Peer, e error) {
	e = r.ParseMessages("peers", (*peers)(&p), peerParser)
	return
}

func parsePeers(bytes []byte) (p []Peer, e error) {
	if bytes == nil {
		return nil, nil
	}

	msg, err := scribe.Parse(bytes)
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing message bytes.")
	}

	return readPeers(msg)
}

// A peer contains the identifying info of a cluster member.
type Peer struct {
	Id   uuid.UUID
	Addr string
}

func newPeer(addr string) Peer {
	return Peer{Id: uuid.NewV1(), Addr: addr}
}

func (p Peer) String() string {
	return fmt.Sprintf("Peer(%v, %v)", p.Id.String()[:8], p.Addr)
}

func (p Peer) ClientPool(ctx common.Context, net net.Network, timeout time.Duration, num int) common.ObjectPool {
	return common.NewObjectPool(ctx.Control(), num, func() (io.Closer, error) {
		cl, err := p.Client(ctx, net, timeout)
		if err == nil {
			return cl, nil
		} else {
			return nil, err
		}
	})
}

func (p Peer) Client(ctx common.Context, network net.Network, timeout time.Duration) (*rpcClient, error) {
	raw, err := network.Dial(timeout, p.Addr)
	if raw == nil || err != nil {
		return nil, errors.Wrapf(err, "Error connecting to peer [%v]", p)
	}

	cl, err := net.NewClient(ctx, raw, net.Json)
	if cl == nil || err != nil {
		return nil, err
	}

	return &rpcClient{cl}, nil
}

// func (p peer) Connect(ctx common.Context, network net.Network) (*rpcClient, error) {
// // exponential backoff up to 2^6 seconds.
// for timeout := 1 * time.Second; ; {
// ch := make(chan *rpcClient)
// go func() {
// cl, err := p.Client(ctx)
// if err == nil && cl != nil {
// ch <- cl
// }
// }()
//
// timer := time.NewTimer(timeout)
// select {
// case <-cancel:
// return nil, ClosedError
// case <-timer.C:
//
// // 64 seconds is maximum timeout
// if timeout < 2^6*time.Second {
// timeout *= 2
// }
//
// continue
// case cl := <-ch:
// return cl, nil
// }
// }
// }

func (p Peer) Write(w scribe.Writer) {
	w.WriteUUID("id", p.Id)
	w.WriteString("addr", p.Addr)
}

func peerParser(r scribe.Reader) (interface{}, error) {
	var p Peer
	var e error
	e = common.Or(e, r.ReadUUID("id", &p.Id))
	e = common.Or(e, r.ReadString("addr", &p.Addr))
	return p, e
}
