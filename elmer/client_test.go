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

	cl, err := Connect(ctx, collectAddrs(cluster))
	assert.Nil(t, err)

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
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	cluster := NewTestCluster(ctx, 3)
	assert.Equal(t, 3, len(cluster))

	cl, err := Connect(ctx, collectAddrs(cluster))
	assert.Nil(t, err)

	catalog, err := cl.Catalog()
	assert.Nil(t, err)

	store, err := catalog.Ensure(timer.Closed(), []byte("store"))
	assert.Nil(t, err)

	numThreads := 10
	numItemsPerThread := 100
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			for j := 0; j < numItemsPerThread; j++ {
				store.Put(timer.Closed(), stash.IntBytes(numThreads*i+j), stash.IntBytes(numThreads*i+j), 0)
			}
		}(i)
	}

	sync, err := cluster[0].self.Sync()
	assert.Nil(t, sync.Sync(timer.Closed(), numThreads*numItemsPerThread-1))
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
