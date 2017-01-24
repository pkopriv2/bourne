package kayak

import (
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// replicated configuration
type peers []peer

func (p peers) Write(w scribe.Writer) {
	w.WriteMessage("peers", p)
}

func readPeers(r scribe.Reader) (p peers, e error) {
	e = r.ParseMessages("peers", &p, peerParser)
	return
}

// A peer contains the identifying info of a cluster member.
type peer struct {
	id   uuid.UUID
	addr string
}

func newPeer(addr string) peer {
	return peer{id: uuid.NewV1(), addr: addr}
}

func (p peer) String() string {
	return fmt.Sprintf("Peer(%v, %v)", p.id.String()[:8], p.addr)
}

func (p peer) Client(ctx common.Context) (*client, error) {
	raw, err := net.NewTcpClient(ctx, ctx.Logger(), p.addr)
	if err != nil {
		return nil, err
	}

	return &client{raw}, nil
}

func (p peer) Write(w scribe.Writer) {
	w.WriteUUID("id", p.id)
	w.WriteString("addr", p.addr)
}

func peerParser(r scribe.Reader) (interface{}, error) {
	var p peer
	var e error
	e = common.Or(e, r.ReadUUID("id", &p.id))
	e = common.Or(e, r.ReadString("addr", &p.addr))
	return p, e
}
