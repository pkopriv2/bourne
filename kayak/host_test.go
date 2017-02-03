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

	host, err := StartTransient(ctx, ":0")
	assert.Nil(t, err)

	time.Sleep(5 * time.Second)
	assert.Nil(t, host.Close())
	time.Sleep(5 * time.Second)

	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	assert.Equal(t, before, runtime.NumGoroutine())
}

func TestHost_Cluster_ConvergeTwoPeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster, err := StartTransientCluster(ctx, 2)
	assert.Nil(t, err)
	assert.NotNil(t, Converge(ctx.Timer(10*time.Second), cluster))
}

func TestHost_Cluster_ConvergeThreePeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster, err := StartTransientCluster(ctx, 3)
	assert.Nil(t, err)
	assert.NotNil(t, Converge(ctx.Timer(10*time.Second), cluster))
}

func TestHost_Cluster_ConvergeFivePeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster, err := StartTransientCluster(ctx, 5)
	assert.Nil(t, err)
	assert.NotNil(t, Converge(ctx.Timer(10*time.Second), cluster))
}

func TestHost_Cluster_ConvergeSevenPeers(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()
	cluster, err := StartTransientCluster(ctx, 7)
	assert.Nil(t, err)
	assert.NotNil(t, Converge(ctx.Timer(10*time.Second), cluster))
}

func TestHost_Cluster_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	before := runtime.NumGoroutine()
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

	cluster, err := StartTransientCluster(ctx, 7)
	assert.Nil(t, err)
	assert.NotNil(t, Converge(ctx.Timer(5*time.Second), cluster))

	time.Sleep(5 * time.Second)
	ctx.Close()
	time.Sleep(5 * time.Second)

	runtime.GC()
	time.Sleep(5 * time.Second)

	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	assert.Equal(t, before, runtime.NumGoroutine())
}

func TestHost_Cluster_Leader_Failure(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})
	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTransientCluster(ctx, 3)
	assert.Nil(t, err)

	leader := Converge(ctx.Timer(10*time.Second), cluster)
	assert.NotNil(t, leader)

	leader.Close()
	ctx.Logger().Info("Leader killed. Waiting for re-election")
	time.Sleep(5 * time.Second)

	after := Converge(ctx.Timer(10*time.Second), cluster)

	assert.NotNil(t, after)
	assert.NotEqual(t, leader, after)
}

func TestHost_Cluster_Leader_Append_Single(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTransientCluster(ctx, 3)
	assert.Nil(t, err)

	leader := Converge(ctx.Timer(10*time.Second), cluster)
	assert.NotNil(t, leader)

	item, err := leader.(*host).core.Append(Event{0, 1}, Std)
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	SyncMajority(timer, cluster, SyncTo(item.Index))
	assert.False(t, common.IsCanceled(timer))
}

func TestHost_Cluster_Leader_Append_Multi(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTransientCluster(ctx, 7)
	assert.Nil(t, err)

	leader := Converge(ctx.Timer(10*time.Second), cluster)
	assert.NotNil(t, leader)

	numThreads := 10
	numItemsPerThread := 10

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

	timer := ctx.Timer(30 * time.Second)
	SyncMajority(timer, cluster, SyncTo(numThreads*numItemsPerThread-1))
	assert.False(t, common.IsCanceled(timer))
}

func SyncTo(index int) func(p Peer) bool {
	return func(p Peer) bool {
		log, err := p.Log()
		if err != nil {
			return false
		}

		return log.Head() >= index && log.Committed() >= index
	}
}

func TestHost_Cluster_Leader_Append_WithCompactions(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTransientCluster(ctx, 3)
	assert.Nil(t, err)

	leader := Converge(ctx.Timer(10*time.Second), cluster)
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

	timer := ctx.Timer(30 * time.Second)
	SyncMajority(timer, cluster, SyncTo(numThreads*numItemsPerThread-1))
	assert.False(t, common.IsCanceled(timer))
}

func TestHost_Cluster_Append_Single(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTransientCluster(ctx, 3)
	assert.Nil(t, err)

	leader := Converge(ctx.Timer(10*time.Second), cluster)
	assert.NotNil(t, leader)

	log, err := leader.Log()
	assert.Nil(t, err)

	item, err := log.Append(nil, Event{0})
	assert.Nil(t, err)

	timer := ctx.Timer(10 * time.Second)
	SyncMajority(timer, cluster, SyncTo(item.Index))
	assert.False(t, common.IsCanceled(timer))
}

func TestCluster_Append_Multi(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTransientCluster(ctx, 3)
	assert.Nil(t, err)

	leader := Converge(ctx.Timer(10*time.Second), cluster)
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

	timer := ctx.Timer(30 * time.Second)
	SyncMajority(timer, cluster, SyncTo(numThreads*numItemsPerThread-1))
	assert.False(t, common.IsCanceled(timer))
}

func TestHost_Cluster_Barrier(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	cluster, err := StartTransientCluster(ctx, 3)
	assert.Nil(t, err)

	leader := Converge(ctx.Timer(10*time.Second), cluster)
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

	timer := ctx.Timer(30 * time.Second)
	SyncMajority(timer, cluster, SyncTo(numThreads*numItemsPerThread-1))
	assert.False(t, common.IsCanceled(timer))
}
