package elmer

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/stash"
	"github.com/stretchr/testify/assert"
)

func TestClient_StoreCreate_NoExist(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	cluster := NewTestCluster(ctx, 3)
	assert.Equal(t, 3, len(cluster))

	cl, err := Connect(ctx, collectAddrs(cluster))
	assert.Nil(t, err)

	root, err := cl.Root()
	assert.Nil(t, err)

	store, err := root.CreateStore(timer.Closed(), []byte("store"))
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

	root, err := cl.Root()
	assert.Nil(t, err)

	store, err := root.CreateStore(timer.Closed(), []byte("store"))
	assert.Nil(t, err)

	val := make([]byte, 1024)

	numThreads := 1
	numItemsPerThread := 1000
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			for j := 0; j < numItemsPerThread; j++ {
				store.Put(timer.Closed(), stash.IntBytes(numThreads*i+j), val, 0)
			}
		}(i)
	}

	sync, err := cluster[0].self.Sync()
	assert.Nil(t, sync.Sync(timer.Closed(), numThreads*numItemsPerThread-1))
}

func TestClient_Catalog_Store_Increment(t *testing.T) {
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

	root, err := cl.Root()
	assert.Nil(t, err)

	store, err := root.CreateStore(timer.Closed(), []byte("store"))
	assert.Nil(t, err)

	numThreads := 3
	numIncPerThread := 3
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			for j := 0; j < numIncPerThread; j++ {
				val, err := Inc(store, timer.Closed(), []byte("key"))
				if err != nil {
					ctx.Logger().Info("Error: %+v", err)
					t.Fail()
					return
				}

				ctx.Logger().Info("Incremented: %v", val)
			}
		}(i)
	}

	sync, err := cluster[0].self.Sync()
	assert.Nil(t, sync.Sync(timer.Closed(), numThreads*numIncPerThread-1))
}

func Inc(store Store, cancel <-chan struct{}, key []byte) (int, error) {
	for {
		item, ok, err := store.Get(cancel, key)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		var cur int
		if ok {
			tmp, err := stash.ParseInt(item.Val)
			if err != nil {
				return 0, errors.WithStack(err)
			}
			cur = tmp
		} else {
			cur = 0
		}

		cur++

		_, ok, err = store.Put(cancel, key, stash.IntBytes(cur), item.Ver)
		if err != nil {
			return 0, errors.WithStack(err)
		}

		if ok {
			return cur, nil
		}
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
