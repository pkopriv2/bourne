package kayak

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/stash"
	"github.com/stretchr/testify/assert"
)

func TestHost_Close(t *testing.T) {
	ctx := common.NewEmptyContext()

	before := runtime.NumGoroutine()
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

	stg, err := TestStorage(ctx)
	assert.Nil(t, err)

	host, err := Start(ctx, ":0", stg)
	assert.Nil(t, err)

	time.Sleep(5 * time.Second)
	assert.Nil(t, host.Close())
	time.Sleep(5 * time.Second)

	runtime.GC()
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	assert.True(t, before <= runtime.NumGoroutine())
}

func TestHost_ReadSnapshot_Empty(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	host, err := StartTestHost(ctx)
	assert.Nil(t, err)

	log, err := host.Log()
	assert.Nil(t, err)

	index, stream, err := log.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, -1, index)

	select {
	case <-stream.Ctrl().Closed():
	case <-stream.Data():
		assert.Fail(t, "Should not have received item")
	}
}

func TestHost_ReadSnapshot(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		Config.BaseElectionTimeout: 500 * time.Millisecond,
	})
	ctx := common.NewContext(conf)
	defer ctx.Close()

	timer := ctx.Timer(30*time.Second)

	host, err := StartTestHost(ctx)
	assert.Nil(t, err)

	log, err := host.Log()
	assert.Nil(t, err)
	for i := 0; i < 3; i++ {
		log.Append(timer.Closed(), stash.IntBytes(i))
	}

	assert.Nil(t, log.Compact(1, NewEventChannel([]Event{}), 0))

	index, stream, err := log.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, 1, index)

	select {
	case <-stream.Ctrl().Closed():
	case <-stream.Data():
		assert.Fail(t, "Should not received item")
	}
}

func TestHost_Cluster_ConvergeTwoPeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 2)
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()
	assert.NotNil(t, ElectLeader(timer.Closed(), cluster))
}

func TestHost_Cluster_ConvergeThreePeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()
	assert.NotNil(t, ElectLeader(timer.Closed(), cluster))
}

func TestHost_Cluster_ConvergeFivePeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster, err := StartTestCluster(ctx, 5)
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()
	assert.NotNil(t, ElectLeader(timer.Closed(), cluster))
}

func TestHost_Cluster_ConvergeSevenPeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster, err := StartTestCluster(ctx, 7)
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()
	assert.NotNil(t, ElectLeader(timer.Closed(), cluster))
}

func TestHost_Cluster_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	before := runtime.NumGoroutine()
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

	cluster, err := StartTestCluster(ctx, 7)
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()
	assert.NotNil(t, ElectLeader(timer.Closed(), cluster))

	time.Sleep(5 * time.Second)
	ctx.Close()
	time.Sleep(5 * time.Second)

	runtime.GC()
	time.Sleep(5 * time.Second)

	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	assert.True(t, before >= runtime.NumGoroutine())
}

func TestHost_Cluster_Leader_Failure(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})
	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	timer := ctx.Timer(20 * time.Second)
	defer timer.Close()

	leader := ElectLeader(timer.Closed(), cluster)
	assert.NotNil(t, leader)

	leader.Close()
	ctx.Logger().Info("Leader killed. Waiting for re-election")
	time.Sleep(5 * time.Second)

	after := ElectLeader(timer.Closed(), cluster)

	assert.NotNil(t, after)
	assert.NotEqual(t, leader, after)
}

func TestHost_Cluster_Leader_Append_Single(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()

	leader := ElectLeader(timer.Closed(), cluster)
	assert.NotNil(t, leader)

	item, err := leader.(*host).core.Append(Event{0, 1}, Std)
	assert.Nil(t, err)

	SyncMajority(timer.Closed(), cluster, SyncTo(item.Index))
	assert.False(t, common.IsCanceled(timer.Closed()))
}

func TestHost_Cluster_Leader_Append_Multi(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	timer := ctx.Timer(60 * time.Second)
	defer timer.Close()

	leader := ElectLeader(timer.Closed(), cluster)
	assert.NotNil(t, leader)

	numThreads := 10
	numItemsPerThread := 100

	for i := 0; i < numThreads; i++ {
		go func() {
			for j := 0; j < numItemsPerThread; j++ {
				_, err := leader.(*host).core.Append(Event(stash.Int(numThreads*i+j)), Std)
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	SyncMajority(timer.Closed(), cluster, SyncTo(numThreads*numItemsPerThread-1))
	assert.False(t, common.IsCanceled(timer.Closed()))
}

func TestHost_Cluster_Leader_Append_WithCompactions(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	leader := ElectLeader(timer.Closed(), cluster)
	assert.NotNil(t, leader)

	numThreads := 10
	numItemsPerThread := 100

	events := make([]Event, 0, 1024)
	for i := 0; i < 1024; i++ {
		events = append(events, Event(stash.Int(1)))
	}

	go func() {
		for range time.NewTicker(100 * time.Millisecond).C {
			if common.IsClosed(ctx.Control().Closed()) {
				return
			}

			for _, p := range cluster {
				log, err := p.Log()
				if err != nil {
					panic(err)
				}
				p.Context().Logger().Info("Running compactions: %v", log.Compact(log.Committed(), NewEventChannel(events), len(events)))
			}
		}
	}()

	for i := 0; i < numThreads; i++ {
		go func() {
			for j := 0; j < numItemsPerThread; j++ {
				_, err := leader.(*host).core.Append(Event(stash.Int(numThreads*i+j)), Std)
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	SyncMajority(timer.Closed(), cluster, SyncTo(numThreads*numItemsPerThread-1))
	assert.False(t, common.IsCanceled(timer.Closed()))
}

func TestHost_Cluster_Append_Single(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	leader := ElectLeader(timer.Closed(), cluster)
	assert.NotNil(t, leader)

	log, err := leader.Log()
	assert.Nil(t, err)

	item, err := log.Append(nil, Event{0})
	assert.Nil(t, err)

	SyncMajority(timer.Closed(), cluster, SyncTo(item.Index))
	assert.False(t, common.IsCanceled(timer.Closed()))
}

func TestCluster_Append_Multi(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	leader := ElectLeader(timer.Closed(), cluster)
	assert.NotNil(t, leader)

	log, err := leader.Log()
	assert.Nil(t, err)

	numThreads := 100
	numItemsPerThread := 100

	for i := 0; i < numThreads; i++ {
		go func() {
			for j := 0; j < numItemsPerThread; j++ {
				_, err := log.Append(nil, Event(stash.Int(numThreads*i+j)))
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	SyncMajority(timer.Closed(), cluster, SyncTo(numThreads*numItemsPerThread-1))
	assert.False(t, common.IsCanceled(timer.Closed()))
}

func TestHost_Cluster_Barrier(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	assert.Nil(t, err)

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	leader := ElectLeader(timer.Closed(), cluster)
	assert.NotNil(t, leader)

	log, err := leader.Log()
	assert.Nil(t, err)

	sync, err := leader.Sync()
	assert.Nil(t, err)

	numThreads := 10
	numItemsPerThread := 100

	max := concurrent.NewAtomicCounter()
	for i := 0; i < numThreads; i++ {
		go func() {
			for j := 0; j < numItemsPerThread; j++ {
				item, err := log.Append(nil, Event(stash.Int(numThreads*i+j)))
				if err != nil {
					panic(err)
				}

				max.Update(func(cur uint64) uint64 {
					return uint64(common.Max(int(cur), item.Index))
				})
			}
		}()
	}

	go func() {
		for {
			val, err := sync.Barrier(nil)
			assert.Nil(t, err)
			cur := max.Get()
			assert.True(t, uint64(val) <= cur)
			leader.Context().Logger().Info("Barrier(val=%v, max=%v)", val, cur)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	SyncMajority(timer.Closed(), cluster, SyncTo(numThreads*numItemsPerThread-1))
	assert.False(t, common.IsCanceled(timer.Closed()))
}

func TestHost_Cluster_Join_Busy(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})
	ctx := common.NewContext(conf)
	defer ctx.Close()

	timer := ctx.Timer(30*time.Second)

	cluster, err := StartTestCluster(ctx, 2)
	assert.Nil(t, err)

	leader := ElectLeader(timer.Closed(), cluster)
	assert.NotNil(t, leader)

	log, err := leader.Log()
	assert.Nil(t, err)

	numThreads := 100
	numItemsPerThread := 100

	for i := 0; i < numThreads; i++ {
		go func() {
			for j := 0; j < numItemsPerThread; j++ {
				_, err := log.Append(timer.Closed(), Event(stash.Int(numThreads*i+j)))
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	SyncMajority(timer.Closed(), cluster, SyncTo(numThreads*numItemsPerThread/4))
	assert.Nil(t, err)

	joined, err := JoinTestHost(ctx, leader.Addr())
	assert.Nil(t, err)

	SyncMajority(timer.Closed(), []Host{joined}, SyncTo(numThreads*numItemsPerThread-1))
}

// func benchmarkCluster_Append(log Log, threads int, itemsPerThread int, t *testing.B) {
// for i := 0; i < threads; i++ {
// go func() {
// for j := 0; j < itemsPerThread; j++ {
// for k := 0; k < t.N; k++ {
// _, err := log.Append(nil, Event(stash.Int(threads*i+j)))
// if err != nil {
// panic(err)
// }
// }
// }
// }()
// }
// }
//
// func Benchmark_Append_3_10_10(t *testing.B) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Debug),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// cluster, err := TestCluster(ctx, 3)
// if err != nil {
// t.FailNow()
// }
//
// leader := Converge(nil, cluster)
// if leader == nil {
// t.FailNow()
// }
//
// log, err := leader.Log()
// if err != nil {
// t.FailNow()
// }
//
// t.ResetTimer()
// leader.(*host).logger.Info("Starting benchmark (peers=3,threads=10,items=10)")
// benchmarkCluster_Append(log, 10, 10, t)
// }

func SyncTo(index int) func(p Host) bool {
	return func(p Host) bool {
		log, err := p.Log()
		if err != nil {
			return false
		}

		return log.Head() >= index && log.Committed() >= index
	}
}
