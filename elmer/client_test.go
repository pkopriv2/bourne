package elmer

import (
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/stash"
	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	t.Run("Client_Basic_1", testClient_Basic(t, 1))
	t.Run("Client_Basic_2", testClient_Basic(t, 2))
	t.Run("Client_Basic_3", testClient_Basic(t, 3))
	t.Run("Client_Basic_4", testClient_Basic(t, 4))
	t.Run("Client_Basic_5", testClient_Basic(t, 5))
}

func testClient_Basic(t *testing.T, size int) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := common.NewEmptyContext()
		defer ctx.Close()

		cluster, err := NewTestCluster(ctx, size)
		if err != nil {
			t.Fail()
			return
		}

		cl, err := NewTestClient(ctx, cluster)
		if err != nil {
			t.Fail()
			return
		}

		root, err := cl.Root()
		if err != nil {
			t.Fail()
			return
		}

		timer := ctx.Timer(30 * time.Second)
		defer timer.Close()

		t.Run("GetStore_NoExist", testClient_GetStore_NoExist(ctx, timer.Closed(), root))
		t.Run("CreateStore_Simple", testClient_CreateStore_Simple(ctx, timer.Closed(), root))
		t.Run("CreateStore_AlreadyExists", testClient_CreateStore_AlreadyExists(ctx, timer.Closed(), root))
		t.Run("CreateStore_PreviouslyDeleted", testClient_CreateStore_PreviouslyDeleted(ctx, timer.Closed(), root))

		t.Run("DeleteStore_NoExist", testClient_DeleteStore_NoExist(ctx, timer.Closed(), root))
		t.Run("DeleteStore_Simple", testClient_DeleteStore_Simple(ctx, timer.Closed(), root))
		t.Run("DeleteStore_PreviouslyDeleted", testClient_DeleteStore_PreviouslyDeleted(ctx, timer.Closed(), root))

		t.Run("StoreRead_NoExist", testClient_StoreRead_NoExist(ctx, timer.Closed(), root))
		t.Run("StoreRead_Simple", testClient_StoreRead_Simple(ctx, timer.Closed(), root))
		t.Run("StoreSwap_Atomicity", testClient_StoreSwap_Atomicity(ctx, timer.Closed(), root))

		t.Run("Close", testClient_Close(ctx, timer.Closed(), cl))
	}
}

// These testClient_s are independent of cluster topology/behavior.
func testClient_StoreRead_NoExist(ctx common.Context, cancel <-chan struct{}, root Store) func(t *testing.T) {
	return func(t *testing.T) {
		_, ok, err := root.Get(cancel, []byte("key"))
		assert.Nil(t, err)
		assert.False(t, ok)
	}
}

func testClient_StoreRead_Simple(ctx common.Context, cancel <-chan struct{}, root Store) func(t *testing.T) {
	return func(t *testing.T) {
		item, ok, err := root.Put(cancel, []byte("key"), []byte("val"), -1)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("key"), item.Key)
		assert.Equal(t, []byte("val"), item.Val)
		assert.Equal(t, 0, item.Ver)

		read, ok, err := root.Get(cancel, []byte("key"))
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, item, read)
	}
}

func testClient_StoreSwap_Atomicity(ctx common.Context, cancel <-chan struct{}, root Store) func(t *testing.T) {
	key := []byte("testClient_StoreSwap_Atomicity")
	return func(t *testing.T) {
		numRoutines := 10
		numIncPerRoutine := 10

		var wait sync.WaitGroup
		for i := 0; i < numRoutines; i++ {
			wait.Add(1)
			go func(i int) {
				defer wait.Done()
				for j := 0; j < numIncPerRoutine; j++ {
					ctx.Logger().Info("Increment: %v", j)
					err := storeIncrementInt(cancel, root, key)
					if err != nil {
						ctx.Logger().Error("Error Incrementing: %+v", err)
					}
				}
			}(i)
		}

		wait.Wait()
		val, _, err := storeReadInt(cancel, root, key)
		assert.Nil(t, err)
		assert.Equal(t, numRoutines*numIncPerRoutine, val)
	}
}

func testClient_Close(ctx common.Context, cancel <-chan struct{}, cl Peer) func(t *testing.T) {
	return func(t *testing.T) {
		assert.Nil(t, cl.Close())
	}
}

func testClient_GetStore_NoExist(ctx common.Context, cancel <-chan struct{}, root Store) func(t *testing.T) {
	return func(t *testing.T) {
		child, err := root.GetStore(cancel, []byte("GetStore_NoExist"))
		assert.Nil(t, err)
		assert.Nil(t, child)
	}
}

func testClient_CreateStore_Simple(ctx common.Context, cancel <-chan struct{}, root Store) func(t *testing.T) {
	return func(t *testing.T) {
		child, err := root.CreateStore(cancel, []byte("testClient_CreateStore_Simple"))
		assert.Nil(t, err)
		assert.NotNil(t, child)
	}
}

func testClient_CreateStore_AlreadyExists(ctx common.Context, cancel <-chan struct{}, root Store) func(t *testing.T) {
	return func(t *testing.T) {
		_, err := root.CreateStore(cancel, []byte("testClient_CreateStore_AlreadyExists"))
		assert.Nil(t, err)

		_, err = root.CreateStore(cancel, []byte("testClient_CreateStore_AlreadyExists"))
		assert.Equal(t, InvariantError, common.Extract(err, InvariantError))
	}
}

func testClient_CreateStore_PreviouslyDeleted(ctx common.Context, cancel <-chan struct{}, root Store) func(t *testing.T) {
	name := []byte("testClient_CreateStore_PreviouslyDeleted")
	return func(t *testing.T) {
		_, err := root.CreateStore(cancel, name)
		assert.Nil(t, err)
		assert.Nil(t, root.DeleteStore(cancel, name))

		_, err = root.CreateStore(cancel, name)
		assert.Nil(t, err)
	}
}

func testClient_DeleteStore_NoExist(ctx common.Context, cancel <-chan struct{}, root Store) func(t *testing.T) {
	return func(t *testing.T) {
		err := root.DeleteStore(cancel, []byte("testClient_DeleteStore_NoExist"))
		assert.Equal(t, InvariantError, common.Extract(err, InvariantError))
	}
}

func testClient_DeleteStore_Simple(ctx common.Context, cancel <-chan struct{}, root Store) func(t *testing.T) {
	name := []byte("testClient_DeleteStore_Simple")
	return func(t *testing.T) {
		_, err := root.CreateStore(cancel, name)
		assert.Nil(t, err)
		assert.Nil(t, root.DeleteStore(cancel, name))

		store, err := root.GetStore(cancel, name)
		assert.Nil(t, err)
		assert.Nil(t, store)
	}
}

func testClient_DeleteStore_PreviouslyDeleted(ctx common.Context, cancel <-chan struct{}, root Store) func(t *testing.T) {
	name := []byte("testClient_DeleteStore_PreviouslyDeleted")
	return func(t *testing.T) {
		_, err := root.CreateStore(cancel, name)
		assert.Nil(t, err)
		assert.Nil(t, root.DeleteStore(cancel, name))

		_, err = root.CreateStore(cancel, name)
		assert.Nil(t, err)
		assert.Nil(t, root.DeleteStore(cancel, name))
	}
}

func collectAddrs(peers []*peer) []string {
	ret := make([]string, 0, len(peers))
	for _, p := range peers {
		ret = append(ret, p.addr)
	}
	return ret
}

func NewTestClient(ctx common.Context, cluster []*peer) (Peer, error) {
	cl, err := Connect(ctx, collectAddrs(cluster))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return cl, nil
}

func NewTestCluster(ctx common.Context, num int) ([]*peer, error) {
	cluster, err := kayak.StartTestCluster(ctx, num)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	kayak.ElectLeader(timer.Closed(), cluster)

	ret := make([]*peer, 0, len(cluster))
	for _, host := range cluster {
		peer, err := newPeer(ctx, host, net.NewTcpNetwork(), ":0")
		if err != nil {
			panic(err)
		}
		ret = append(ret, peer)
	}
	return ret, nil
}

func storeIncrementInt(cancel <-chan struct{}, store Store, key []byte) error {
	_, err := Update(cancel, store, key, func(cur []byte) ([]byte, error) {
		if cur == nil || len(cur) == 0 {
			return stash.IntBytes(1), nil
		}

		num, err := stash.ParseInt(cur)
		if err != nil {
			return nil, err
		}

		return stash.IntBytes(num + 1), nil
	})
	return err
}

func storeReadInt(cancel <-chan struct{}, store Store, key []byte) (int, bool, error) {
	item, ok, err := store.Get(cancel, key)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	if ! ok || item.Del {
		return 0, false, nil
	}

	num, err := stash.ParseInt(item.Val)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	return num, true, nil
}
