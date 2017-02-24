package elmer

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
	"github.com/stretchr/testify/assert"
)

func TestRosterSync_Single(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	peers, err := NewTestCluster(ctx, 1)
	if err != nil {
		t.Fail()
		return
	}

	sync := newRosterSync(ctx, net.NewTcpNetwork(), 30*time.Second, 1*time.Second, collectAddrs(peers))

	roster, err := sync.Roster()
	assert.Nil(t, err)
	assert.Equal(t, collectAddrs(peers), roster)
}

func TestRosterSync_Multiple_NoRefresh(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	peers, err := NewTestCluster(ctx, 3)
	if err != nil {
		t.Fail()
		return
	}

	sync := newRosterSync(ctx, net.NewTcpNetwork(), 30*time.Second, 1*time.Second, collectAddrs(peers))

	roster, err := sync.Roster()
	assert.Nil(t, err)
	assert.Equal(t, collectAddrs(peers), roster)
}

func TestRosterSync_Multiple_WithRefresh(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	raw1, err := kayak.StartTestHost(ctx)
	assert.Nil(t, err)

	peer1, err := Start(ctx, raw1, ":0")
	assert.Nil(t, err)

	sync := newRosterSync(ctx, net.NewTcpNetwork(), 30*time.Second, 1*time.Second, []string{peer1.Addr()})

	roster, err := sync.Roster()
	assert.Nil(t, err)
	assert.Equal(t, collectAddrs([]*peer{peer1.(*peer)}), roster)

	raw2, err := kayak.JoinTestHost(ctx, raw1.Addr())
	assert.Nil(t, err)

	peer2, err := Start(ctx, raw2, ":0")
	assert.Nil(t, err)

	roster, err = sync.Roster()
	assert.Nil(t, err)
	assert.Equal(t, collectAddrs([]*peer{peer1.(*peer), peer2.(*peer)}), roster)

}
