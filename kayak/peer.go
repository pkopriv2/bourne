package kayak

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

func listenRosterChanges(h *replica) {
	go func() {
		defer h.logger.Info("No longer listening for roster changes")

		for {
			h.logger.Info("Reloading roster changes")
			peers, until, err := reloadRoster(h.Log)
			if err != nil {
				h.ctrl.Fail(err)
				return
			}

			h.Roster.Set(peers)

			if err := listenForRosterChanges(h, until+1); err != nil {
				h.logger.Info("Error while listening for roster changes: %v", err)
				if cause := errors.Cause(err); cause != OutOfBoundsError {
					return
				}
				continue
			}
			return
		}
	}()
}


type roster struct {
	raw []peer
	ver *ref
}

func newRoster(init []peer) *roster {
	return &roster{raw: init, ver: newRef(0)}
}

func (c *roster) Wait(next int) ([]peer, int, bool) {
	_, ok := c.ver.WaitExceeds(next)
	peers, ver := c.Get()
	return peers, ver, ok
}

func (c *roster) Notify() {
	c.ver.Notify()
}

func (c *roster) Set(peers []peer) {
	c.ver.Update(func(cur int) int {
		c.raw = peers
		return cur + 1
	})
}

// not taking copy as it is assumed that array is immutable
func (c *roster) Get() (peers []peer, ver int) {
	c.ver.Update(func(cur int) int {
		peers, ver = c.raw, cur
		return cur
	})
	return
}

func (c *roster) Close() {
	c.ver.Close()
}

func hasPeer(peers []peer, p peer) bool {
	for _, cur := range peers {
		if cur.Id == p.Id {
			return true
		}
	}
	return false
}

func findPeer(peers []peer, p peer) int {
	for i, cur := range peers {
		if cur.Id == p.Id {
			return i
		}
	}
	return -1
}

func addPeer(cur []peer, p peer) []peer {
	if hasPeer(cur, p) {
		return cur
	}

	return append(cur, p)
}

func delPeer(cur []peer, p peer) []peer {
	index := findPeer(cur, p)
	if index == -1 {
		return cur
	}

	return append(cur[:index], cur[index+1:]...)
}

func clusterBytes(cluster peers) []byte {
	return cluster.Bytes()
}

func clusterMessage(cluster peers) scribe.Message {
	return scribe.Write(cluster)
}

// replicated configuration
type peers []peer

func (p peers) Write(w scribe.Writer) {
	w.WriteMessages("peers", p)
}

func (p peers) Bytes() []byte {
	return scribe.Write(p).Bytes()
}

func readPeers(r scribe.Reader) (p []peer, e error) {
	e = r.ParseMessages("peers", (*peers)(&p), peerParser)
	return
}

func parsePeers(bytes []byte) (p []peer, e error) {
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

func (p peer) Pool(ctx common.Context) net.ConnectionPool {
	return net.NewConnectionPool("tcp", p.Addr, 10, 2*time.Second)
}

func (p peer) Client(ctx common.Context) (*rpcClient, error) {
	raw, err := net.NewTcpClient(ctx, ctx.Logger(), p.Addr)
	if err != nil {
		return nil, err
	}

	return &rpcClient{raw}, nil
}

func (p peer) Connect(ctx common.Context, cancel <-chan struct{}) (*rpcClient, error) {
	// exponential backoff up to 2^6 seconds.
	for timeout := 1 * time.Second; ; {
		ch := make(chan *rpcClient)
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

// Roster listener stuff.
func reloadRoster(log *eventLog) ([]peer, int, error) {
	snapshot, err := log.Snapshot()
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Error getting snapshot")
	}

	peers, err := parsePeers(snapshot.Config())
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Error parsing config")
	}
	return peers, snapshot.LastIndex(), nil
}

func listenForRosterChanges(h *replica, start int) error {
	h.logger.Info("Listening for roster changes from [%v]", start)

	l, err := h.Log.ListenAppends(start, 256)
	if err != nil {
		return errors.Wrapf(err, "RosterListener: Error registering listener")
	}

	i, o, e := l.Next()
	for ; o ; i, o, e = l.Next() {

		h.logger.Info("Item: %v", i)
		if i.Kind == Config {
			peers, err := parsePeers(i.Event)
			if err != nil {
				h.logger.Error("Error parsing configuration [%v]", peers)
				continue
			}

			h.logger.Info("Updating roster: %v", peers)
			h.Roster.Set(peers)
		}
	}

	return e
}
