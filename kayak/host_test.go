package kayak

import (
	"strconv"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestHost_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	// before := runtime.NumGoroutine()
	host := StartTestSeedHost(ctx, 9390)
	assert.Nil(t, host.Close())
	// after := runtime.NumGoroutine()
	// assert.Equal(t, before, after)
}

func TestHost_Cluster_ConvergeTwoPeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster := StartTestCluster(ctx, 2)
	assert.NotNil(t, Converge(cluster))
}

func TestHost_Cluster_ConvergeThreePeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster := StartTestCluster(ctx, 3)
	assert.NotNil(t, Converge(cluster))
}

func TestHost_Cluster_ConvergeFivePeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster := StartTestCluster(ctx, 5)
	assert.NotNil(t, Converge(cluster))
}

func TestHost_Cluster_ConvergeSevenPeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster := StartTestCluster(ctx, 7)
	assert.NotNil(t, Converge(cluster))
}

func TestHost_Cluster_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	// before := runtime.NumGoroutine()

	cluster := StartTestCluster(ctx, 2)
	assert.NotNil(t, Converge(cluster))

	ctx.Close()

	// after := runtime.NumGoroutine()
	// assert.Equal(t, before, after)
}

func TestHost_Cluster_Leader_Failure(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster := StartTestCluster(ctx, 5)

	leader1 := Converge(cluster)
	assert.NotNil(t, leader1)
	leader1.Close()

	time.Sleep(2 * time.Second)

	leader2 := Converge(RemoveHost(cluster, Index(cluster, func(h *host) bool {
		return h.Id() == leader1.Id()
	})))

	assert.NotNil(t, leader2)
	assert.NotEqual(t, leader1, leader2)
}

func TestHost_Cluster_Leader_Append_Single(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster := StartTestCluster(ctx, 3)
	leader := Converge(cluster)
	assert.NotNil(t, leader)

	_, err := leader.Append(Event{0, 1})
	assert.Nil(t, err)

	done, timeout := concurrent.NewBreaker(2*time.Second, func() {
		SyncMajority(cluster, func(h *host) bool {
			return h.Log().Head() == 0 && h.Log().Committed() == 0
		})
	})

	select {
	case <-done:
	case <-timeout:
		assert.Fail(t, "Timed out waiting for majority to sync")
	}
}

func TestHost_Cluster_Leader_Append_Multi(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster := StartTestCluster(ctx, 3)
	leader := Converge(cluster)
	assert.NotNil(t, leader)

	numThreads := 10
	numItemsPerThread := 10

	for i := 0; i < numThreads; i++ {
		go func() {
			for j := 0; j < numItemsPerThread; j++ {
				_, err := leader.Append(Event(stash.Int(i*j + j)))
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	done, timeout := concurrent.NewBreaker(500*time.Second, func() {
		SyncAll(cluster, func(h *host) bool {
			return h.Log().Head() == (numThreads*numItemsPerThread)-1 && h.Log().Committed() == (numThreads*numItemsPerThread)-1
		})
	})

	select {
	case <-done:
	case <-timeout:
		assert.FailNow(t, "Timed out waiting for majority to sync")
	}
}

func extractIndices(items []LogItem) []int {
	ret := make([]int, 0, len(items))
	for _, i := range items {
		ret = append(ret, i.Index)
	}
	return ret
}

func TestHost_Cluster_Follower_ClientAppend_SingleBatch_SingleItem(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cluster := StartTestCluster(ctx, 3)
	leader := Converge(cluster)
	assert.NotNil(t, leader)

	member := First(cluster, func(h *host) bool {
		return h.Id() != leader.Id()
	})

	cl, err := member.Client()
	assert.Nil(t, err)
	assert.NotNil(t, cl)

	_, err = cl.Append(Event{0})
	assert.Nil(t, err)

	done, timeout := concurrent.NewBreaker(2*time.Second, func() {
		SyncMajority(cluster, func(h *host) bool {
			return h.Log().Head() == 0 && h.Log().Committed() == 0
		})
	})

	select {
	case <-done:
	case <-timeout:
		assert.Fail(t, "Timed out waiting for majority to sync")
	}
}

func StartTestSeedHost(ctx common.Context, port int) *host {
	host, err := newHost(ctx, newPeer(net.NewAddr("localhost", strconv.Itoa(port))), []peer{}, OpenTestStash(ctx))
	if err != nil {
		panic(err)
	}
	return host
}

func StartTestCluster(ctx common.Context, size int) []*host {
	peers := make([]peer, 0, size)
	for i := 0; i < size; i++ {
		peers = append(peers, newPeer(net.NewAddr("localhost", strconv.Itoa(9300+i))))
	}

	stash := OpenTestStash(ctx)

	ctx.Logger().Info("Starting kayak cluster [%v]", peers)
	hosts := make([]*host, 0, len(peers))
	for i, p := range peers {
		others := make([]peer, 0, len(peers)-1)
		others = append(others, peers[:i]...)
		others = append(others, peers[i+1:]...)

		host, err := newHost(ctx, p, others, stash)
		if err != nil {
			panic(err)
		}

		hosts = append(hosts, host)
		ctx.Env().OnClose(func() {
			host.Close()
		})
	}

	return hosts
}

func Converge(cluster []*host) *host {
	var term int = 0
	var leader *uuid.UUID

	cancelled := make(chan struct{})
	done, timeout := concurrent.NewBreaker(10*time.Second, func() {
		SyncAll(cluster, func(h *host) bool {
			select {
			case <-cancelled:
				return true
			default:
			}

			copy := h.core.CurrentTerm()
			if copy.num > term {
				term = copy.num
			}

			if copy.num == term && copy.leader != nil {
				leader = copy.leader
			}

			return leader != nil && copy.leader == leader && copy.num == term
		})
	})

	// data race...

	select {
	case <-done:
		return First(cluster, func(h *host) bool {
			return h.Id() == *leader
		})
	case <-timeout:
		close(cancelled)
		return nil
	}
}

func RemoveHost(cluster []*host, i int) []*host {
	ret := make([]*host, 0, len(cluster)-1)
	ret = append(ret, cluster[:i]...)
	ret = append(ret, cluster[i+1:]...)
	return ret
}

func SyncMajority(cluster []*host, fn func(h *host) bool) {
	done := make(map[uuid.UUID]struct{})
	start := time.Now()

	majority := majority(len(cluster))
	for len(done) < majority {
		for _, r := range cluster {
			id := r.core.replica.Id
			if _, ok := done[id]; ok {
				continue
			}

			if fn(r) {
				done[id] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				r.core.replica.Logger.Info("Still not sync'ed")
			}
		}
		<-time.After(250 * time.Millisecond)
	}
}

func SyncAll(cluster []*host, fn func(h *host) bool) {
	done := make(map[uuid.UUID]struct{})
	start := time.Now()

	for len(done) < len(cluster) {
		for _, r := range cluster {
			id := r.core.replica.Id
			if _, ok := done[id]; ok {
				continue
			}

			if fn(r) {
				done[id] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				r.core.replica.Logger.Info("Still not sync'ed")
			}
		}
		<-time.After(250 * time.Millisecond)
	}
}

func First(cluster []*host, fn func(h *host) bool) *host {
	for _, h := range cluster {
		if fn(h) {
			return h
		}
	}

	return nil
}

func Index(cluster []*host, fn func(h *host) bool) int {
	for i, h := range cluster {
		if fn(h) {
			return i
		}
	}

	return -1
}
