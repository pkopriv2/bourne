package kayak

import (
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// type config struct {
// raw  []peer
// ver  int
// lock *sync.Cond
// }
//
// func (c config)

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
