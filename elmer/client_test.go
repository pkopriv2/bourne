package elmer

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
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

		t.Run("Close", testClient_Close(ctx, timer.Closed(), cl))

	}
}

// These testClient_s are independent of cluster topology/behavior.
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
