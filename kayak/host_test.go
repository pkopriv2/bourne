package kayak

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
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
	ctx := common.NewEmptyContext()

	before := runtime.NumGoroutine()
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	host := NewTestSeedHost(ctx, "localhost:9390")
	host.Start()
	time.Sleep(3 * time.Second)
	assert.Nil(t, host.Close())
	time.Sleep(1 * time.Second)
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	after := runtime.NumGoroutine()
	assert.Equal(t, before, after)
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

	before := runtime.NumGoroutine()
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

	cluster := StartTestCluster(ctx, 7)
	assert.NotNil(t, Converge(cluster))
	ctx.Close()

	time.Sleep(1000 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	assert.Equal(t, before, after)
}

func TestHost_Cluster_Leader_Failure(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})
	ctx := common.NewContext(conf)
	defer ctx.Close()
	cluster := StartTestCluster(ctx, 3)

	leader1 := Converge(cluster)
	assert.NotNil(t, leader1)
	leader1.Close()

	time.Sleep(3 * time.Second)

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

	item, err := leader.core.Append(Event{0, 1}, Std)
	assert.Nil(t, err)

	done, timeout := concurrent.NewBreaker(2*time.Second, func() {
		SyncMajority(cluster, func(h *host) bool {
			return h.core.Log.Head() >= item.Index && h.core.Log.Committed() >= item.Index
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
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster := StartTestCluster(ctx, 3)
	leader := Converge(cluster)
	assert.NotNil(t, leader)

	numThreads := 100
	numItemsPerThread := 100

	for i := 0; i < numThreads; i++ {
		go func() {
			for j := 0; j < numItemsPerThread; j++ {
				_, err := leader.core.Append(Event(stash.Int(numThreads*i+j)), Std)
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	done, timeout := concurrent.NewBreaker(500*time.Second, func() {
		SyncAll(cluster, func(h *host) bool {
			return h.core.Log.Head() >= (numThreads*numItemsPerThread)-1 && h.core.Log.Committed() >= (numThreads*numItemsPerThread)-1
		})
	})

	select {
	case <-done:
	case <-timeout:
		assert.FailNow(t, "Timed out waiting for majority to sync")
	}
}

func TestHost_Cluster_Leader_Append_WithCompactions(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster := StartTestCluster(ctx, 3)
	leader := Converge(cluster)
	assert.NotNil(t, leader)

	numThreads := 10
	numItemsPerThread := 100

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-ctx.Control().Closed():
				return
			case <-ticker.C:
				for _, p := range cluster {
					events := make([]Event, 0, 1024)
					for i := 0; i < 1024; i++ {
						events = append(events, Event(stash.Int(1)))
					}

					err := p.core.Compact(p.core.Log.Committed(), NewEventChannel(events), len(events))
					p.core.logger.Info("Running compactions: %v", err)
				}
			}
		}
	}()

	for i := 0; i < numThreads; i++ {
		go func(i int) {
			for j := 0; j < numItemsPerThread; j++ {
				_, err := leader.core.Append(Event(stash.Int(i*j+j)), Std)
				if err != nil {
					// hrmm....(is this true anymore??)
					panic(fmt.Sprintf("%+v", err))
				}

				// time.Sleep(500 * time.Millisecond)
			}
		}(i)
	}

	done, timeout := concurrent.NewBreaker(500*time.Second, func() {
		SyncAll(cluster, func(h *host) bool {
			return h.core.Log.Head() >= (numThreads*numItemsPerThread)-1 && h.core.Log.Committed() >= (numThreads*numItemsPerThread)-1
		})
	})

	select {
	case <-done:
	case <-timeout:
		assert.FailNow(t, "Timed out waiting for majority to sync")
	}
}

func TestHost_Cluster_Session_Append_Multi(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster := StartTestCluster(ctx, 3)
	leader := Converge(cluster)
	assert.NotNil(t, leader)

	numThreads := 30
	numItemsPerThread := 300

	for i := 0; i < numThreads; i++ {
		go func() {
			log, err := leader.Log()
			if err != nil {
				panic(err)
			}

			for j := 0; j < numItemsPerThread; j++ {
				_, err := log.Append(nil, Event(stash.Int(numThreads*i+j)))
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	done, timeout := concurrent.NewBreaker(500*time.Second, func() {
		SyncAll(cluster, func(h *host) bool {
			return h.core.Log.Head() >= (numThreads*numItemsPerThread)-1 && h.core.Log.Committed() >= (numThreads*numItemsPerThread)-1
		})
	})

	select {
	case <-done:
	case <-timeout:
		assert.FailNow(t, "Timed out waiting for majority to sync")
	}
}

func TestHost_Cluster_Barrier(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Info),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster := StartTestCluster(ctx, 3)
	leader := Converge(cluster)
	assert.NotNil(t, leader)

	member := First(cluster, func(h *host) bool {
		return h.Id() != leader.Id()
	})

	go func() {
		sync, err := member.Sync()
		if err != nil {
			panic(err)
		}

		for {
			val, err := sync.Barrier(nil)
			leader.core.logger.Info("Value: %v, %v", val, err)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	failures := concurrent.NewAtomicCounter()
	numThreads := 100
	numItemsPerThread := 100
	for i := 0; i < numThreads; i++ {
		go func() {
			log, _ := member.Log()
			for j := 0; j < numItemsPerThread; j++ {
				_, err := log.Append(nil, Event(stash.Int(numThreads*i+j)))
				if err != nil {
					failures.Inc()
				}
			}
		}()
	}

	time.Sleep(2000 * time.Millisecond)
	leader.core.logger.Info("Killing leader!")
	leader.Close()

	done, timeout := concurrent.NewBreaker(500*time.Second, func() {
		SyncAll(cluster, func(h *host) bool {
			if h.Id() == leader.Id() {
				return true
			}

			fails := int(failures.Get())
			return h.core.Log.Head() >= (numThreads*numItemsPerThread)-fails-1 && h.core.Log.Committed() >= (numThreads*numItemsPerThread)-fails-1
		})
	})

	select {
	case <-done:
	case <-timeout:
		assert.FailNow(t, "Timed out waiting for majority to sync")
	}
}

func NewTestSeedHost(ctx common.Context, addr string) *host {
	db := OpenTestLogStash(ctx)
	host, err := newHost(ctx, addr, NewBoltStore(db), db)
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
	ctx.Logger().Info("Starting kayak cluster [%v]", peers)

	db := OpenTestLogStash(ctx)

	hosts := make([]*host, 0, len(peers))
	for _, p := range peers {
		host, err := newHost(ctx, p.Addr, NewBoltStore(db), db)
		if err != nil {
			panic(err)
		}

		hosts = append(hosts, host)
		ctx.Control().Defer(func(error) {
			host.Close()
		})
	}

	zero := hosts[0]
	zero.Start()
	Converge(hosts[:1])

	for i, h := range hosts[1:] {
		ctx.Logger().Info("Adding host [%v]", h.core.Self)
		if err := h.Join(zero.core.Self.Addr); err != nil {
			panic(err)
		}

		SyncAll(hosts[:i+2], func(h *host) bool {
			return hasPeer(zero.core.Cluster(), h.core.Self) && equalPeers(zero.core.Cluster(), h.core.Cluster())
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
			if copy.Num > term {
				term = copy.Num
			}

			if copy.Num == term && copy.Leader != nil {
				leader = copy.Leader
			}

			return leader != nil && copy.Leader == leader && copy.Num == term
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
			id := r.core.Id
			if _, ok := done[id]; ok {
				continue
			}

			if fn(r) {
				done[id] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				r.core.logger.Info("Still not sync'ed")
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
			id := r.core.Id
			if _, ok := done[id]; ok {
				continue
			}

			if fn(r) {
				done[id] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				r.core.logger.Info("Still not sync'ed")
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
