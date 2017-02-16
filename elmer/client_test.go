package elmer

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/stash"
	"github.com/stretchr/testify/assert"
)

func TestClient_Catalog(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	cluster := NewTestCluster(ctx, 3)
	assert.Equal(t, 3, len(cluster))

	cl := newPeerClient(ctx, net.NewTcpNetwork(), 30*time.Second, 30*time.Second, collectAddrs(cluster))

	catalog, err := cl.Catalog()
	assert.Nil(t, err)

	store, err := catalog.Ensure(timer.Closed(), []byte("store"))
	assert.Nil(t, err)

	_, ok, err := store.Get(timer.Closed(), []byte("key"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestClient_Catalog_Store_Put(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	cluster := NewTestCluster(ctx, 1)
	assert.Equal(t, 3, len(cluster))

	cl := newPeerClient(ctx, net.NewTcpNetwork(), 30*time.Second, 30*time.Second, collectAddrs(cluster))
	defer cl.Close()

	catalog, err := cl.Catalog()
	assert.Nil(t, err)

	store, err := catalog.Ensure(timer.Closed(), []byte("store"))
	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		ctx.Logger().Info("Putting: %v", i)
		store.Put(timer.Closed(), stash.IntBytes(i), stash.IntBytes(i), 0)
	}
}

func collectAddrs(peers []*peer) []string {
	ret := make([]string, 0, len(peers))
	for _, p := range peers {
		ret = append(ret, p.addr)
	}
	return ret
}

func NewTestCluster(ctx common.Context, num int) []*peer {
	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	cluster, err := kayak.StartTestCluster(ctx, num)
	if err != nil {
		panic(err)
	}

	kayak.ElectLeader(timer.Closed(), cluster)

	ret := make([]*peer, 0, len(cluster))
	for _, host := range cluster {
		peer, err := newPeer(ctx, host, net.NewTcpNetwork(), ":0")
		if err != nil {
			panic(err)
		}
		ret = append(ret, peer)
	}
	return ret
}
