package kayak

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func TestHost_Close(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})
	ctx := common.NewContext(conf)
	defer ctx.Close()

	// before := runtime.NumGoroutine()
	// pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	// host := StartTestSeedHost(ctx, ":9390")

	// assert.Equal(t, before, after)
	// time.Sleep(100*time.Second)
	// assert.Nil(t, host.Close())
	// time.Sleep(100 * time.Millisecond)
	// pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	// after := runtime.NumGoroutine()
	host1 := NewTestSeedHost(ctx, "localhost:9390")
	host2 := NewTestSeedHost(ctx, "localhost:9391")

	host1.becomeFollower()
	host2.becomeInitiate()

	assert.Nil(t, host1.core.UpdateRoster(rosterUpdate{host2.core.Self, true}))
	time.Sleep(100 * time.Second)
}

// func TestHost_Cluster_ConvergeTwoPeers(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
// cluster := StartTestCluster(ctx, 2)
// assert.NotNil(t, Converge(cluster))
// }
//
// func TestHost_Cluster_ConvergeThreePeers(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
// cluster := StartTestCluster(ctx, 3)
// assert.NotNil(t, Converge(cluster))
// }
//
// func TestHost_Cluster_ConvergeFivePeers(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
// cluster := StartTestCluster(ctx, 5)
// assert.NotNil(t, Converge(cluster))
// }
//
// func TestHost_Cluster_ConvergeSevenPeers(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
// cluster := StartTestCluster(ctx, 7)
// assert.NotNil(t, Converge(cluster))
// }
//
// func TestHost_Cluster_Close(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
//
// before := runtime.NumGoroutine()
// pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
//
// cluster := StartTestCluster(ctx, 3)
// assert.NotNil(t, Converge(cluster))
//
// ctx.Close()
// time.Sleep(1000 * time.Millisecond)
// pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
// after := runtime.NumGoroutine()
// assert.Equal(t, before, after)
// }
//
// func TestHost_Cluster_Leader_Failure(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Debug),
// })
// ctx := common.NewContext(conf)
// defer ctx.Close()
// cluster := StartTestCluster(ctx, 3)
//
// leader1 := Converge(cluster)
// assert.NotNil(t, leader1)
// leader1.Close()
//
// time.Sleep(3 * time.Second)
//
// leader2 := Converge(RemoveHost(cluster, Index(cluster, func(h *host) bool {
// return h.Id() == leader1.Id()
// })))
//
// assert.NotNil(t, leader2)
// assert.NotEqual(t, leader1, leader2)
// }
//
// func TestHost_Cluster_Leader_Append_Single(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Debug),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// cluster := StartTestCluster(ctx, 3)
// leader := Converge(cluster)
// assert.NotNil(t, leader)
//
// _, err := leader.Append(Event{0, 1})
// assert.Nil(t, err)
//
// done, timeout := concurrent.NewBreaker(2*time.Second, func() {
// SyncMajority(cluster, func(h *host) bool {
// return h.Log().Head() == 0 && h.Log().Committed() == 0
// })
// })
//
// select {
// case <-done:
// case <-timeout:
// assert.Fail(t, "Timed out waiting for majority to sync")
// }
// }
//
// func TestHost_Cluster_Leader_Append_Multi(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Debug),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// cluster := StartTestCluster(ctx, 3)
// leader := Converge(cluster)
// assert.NotNil(t, leader)
//
// numThreads := 10
// numItemsPerThread := 10
//
// for i := 0; i < numThreads; i++ {
// go func() {
// for j := 0; j < numItemsPerThread; j++ {
// _, err := leader.Append(Event(stash.Int(numThreads*i + j)))
// if err != nil {
// panic(err)
// }
// }
// }()
// }
//
// done, timeout := concurrent.NewBreaker(500*time.Second, func() {
// SyncAll(cluster, func(h *host) bool {
// return h.Log().Head() == (numThreads*numItemsPerThread)-1 && h.Log().Committed() == (numThreads*numItemsPerThread)-1
// })
// })
//
// select {
// case <-done:
// case <-timeout:
// assert.FailNow(t, "Timed out waiting for majority to sync")
// }
// }
//
// func TestHost_Cluster_Follower_AppendMulti_WithLeaderFailure(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Debug),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// cluster := StartTestCluster(ctx, 3)
// leader := Converge(cluster)
// assert.NotNil(t, leader)
//
// numThreads := 10
// numItemsPerThread := 10
//
// member := First(cluster, func(h *host) bool {
// return h.Id() != leader.Id()
// })
//
// go func() {
// time.Sleep(10 * time.Millisecond)
// leader.logger.Info("Killing leader!")
// leader.Close()
//
// }()
//
// failures := concurrent.NewAtomicCounter()
// for i := 0; i < numThreads; i++ {
// go func(i int) {
// for j := 0; j < numItemsPerThread; j++ {
// _, err := member.Append(Event(stash.Int(numThreads*i + j)))
// if err != nil {
// failures.Inc()
// }
// }
// }(i)
// }
//
// done, timeout := concurrent.NewBreaker(50*time.Second, func() {
// SyncAll(cluster, func(h *host) bool {
// if h.Self() == leader.Self() {
// return true
// }
// return h.Log().Head() >= (numThreads*numItemsPerThread)-int(failures.Get()) && h.Log().Committed() >= (numThreads*numItemsPerThread)-int(failures.Get())
// })
// })
//
// select {
// case <-done:
// case <-timeout:
// assert.FailNow(t, "Timed out waiting for majority to sync")
// }
// }
//
// func TestHost_Cluster_Leader_Append_WithCompactions(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Info),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// cluster := StartTestCluster(ctx, 3)
// leader := Converge(cluster)
// assert.NotNil(t, leader)
//
// numThreads := 10
// numItemsPerThread := 100
//
// go func() {
// ticker := time.NewTicker(100 * time.Millisecond)
// for {
// select {
// case <-ctx.Env().Closed():
// return
// case <-ticker.C:
// leader.logger.Info("Running compactions")
// for _, p := range cluster {
// leader.logger.Error("Error: %v",
// p.Log().Compact(p.Log().Head()-10, NewEventChannel([]Event{Event(stash.Int(1))}), 1, []byte{}))
// }
// }
// }
// }()
//
// for i := 0; i < numThreads; i++ {
// go func(i int) {
// for j := 0; j < numItemsPerThread; j++ {
// _, err := leader.Append(Event(stash.Int(i*j + j)))
// if err != nil {
// panic(err)
// }
// }
// }(i)
// }
//
// done, timeout := concurrent.NewBreaker(500*time.Second, func() {
// SyncAll(cluster, func(h *host) bool {
// return h.Log().Head() == (numThreads*numItemsPerThread)-1 && h.Log().Committed() == (numThreads*numItemsPerThread)-1
// })
// })
//
// select {
// case <-done:
// case <-timeout:
// assert.FailNow(t, "Timed out waiting for majority to sync")
// }
//
// // leader.Log().stash.View(func(tx *bolt.Tx) error {
// // numSegments := 0
// // c := tx.Bucket(segBucket).Cursor()
// // for _, v := c.First(); v != nil; _, v = c.Next() {
// // numSegments++
// // }
// // assert.Equal(t, len(cluster), numSegments)
// //
// // numItems := 0
// // c = tx.Bucket(segItemsBucket).Cursor()
// // for _, v := c.First(); v != nil; _, v = c.Next() {
// // numItems++
// // }
// // assert.Equal(t, len(cluster)*10, numItems)
// //
// // numSnapshots := 0
// // c = tx.Bucket(snapshotsBucket).Cursor()
// // for _, v := c.First(); v != nil; _, v = c.Next() {
// // numSnapshots++
// // }
// // assert.Equal(t, len(cluster), numSnapshots)
// //
// // numEvents := 0
// // c = tx.Bucket(snapshotEventsBucket).Cursor()
// // for _, v := c.First(); v != nil; _, v = c.Next() {
// // numEvents++
// // }
// // assert.Equal(t, len(cluster), numEvents)
// // return nil
// // })
// }
//
// func extractIndices(items []LogItem) []int {
// ret := make([]int, 0, len(items))
// for _, i := range items {
// ret = append(ret, i.Index)
// }
// return ret
// }
//
// func TestHost_Cluster_Follower_ClientAppend_SingleBatch_SingleItem(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// cluster := StartTestCluster(ctx, 3)
// leader := Converge(cluster)
// assert.NotNil(t, leader)
//
// member := First(cluster, func(h *host) bool {
// return h.Id() != leader.Id()
// })
//
// cl, err := member.Client()
// assert.Nil(t, err)
// assert.NotNil(t, cl)
//
// _, err = cl.Append(Event{0, 1})
// assert.Nil(t, err)
//
// done, timeout := concurrent.NewBreaker(2*time.Second, func() {
// SyncMajority(cluster, func(h *host) bool {
// return h.Log().Head() == 0 && h.Log().Committed() == 0
// })
// })
//
// select {
// case <-done:
// case <-timeout:
// assert.Fail(t, "Timed out waiting for majority to sync")
// }
// }
//
func NewTestSeedHost(ctx common.Context, addr string) *host {
	db := OpenTestLogStash(ctx)
	host, err := newHost(ctx, addr, NewBoltStore(db), db)
	if err != nil {
		panic(err)
	}
	return host
}

//
// func StartTestCluster(ctx common.Context, size int) []*host {
// peers := make([]peer, 0, size)
// for i := 0; i < size; i++ {
// peers = append(peers, newPeer(net.NewAddr("localhost", strconv.Itoa(9300+i))))
// }
//
// stash := OpenTestStash(ctx)
//
// ctx.Logger().Info("Starting kayak cluster [%v]", peers)
// hosts := make([]*host, 0, len(peers))
// for i, p := range peers {
// others := make([]peer, 0, len(peers)-1)
// others = append(others, peers[:i]...)
// others = append(others, peers[i+1:]...)
//
// host, err := newHost(ctx, p, others, stash)
// if err != nil {
// panic(err)
// }
//
// hosts = append(hosts, host)
// ctx.Env().OnClose(func() {
// host.Close()
// })
// }
//
// return hosts
// }
//
// func Converge(cluster []*host) *host {
// var term int = 0
// var leader *uuid.UUID
//
// cancelled := make(chan struct{})
// done, timeout := concurrent.NewBreaker(10*time.Second, func() {
// SyncAll(cluster, func(h *host) bool {
// select {
// case <-cancelled:
// return true
// default:
// }
//
// copy := h.core.CurrentTerm()
// if copy.num > term {
// term = copy.num
// }
//
// if copy.num == term && copy.leader != nil {
// leader = copy.leader
// }
//
// return leader != nil && copy.leader == leader && copy.num == term
// })
// })
//
// // data race...
//
// select {
// case <-done:
// return First(cluster, func(h *host) bool {
// h.logger.Info("SEARCHING FOR LEADER: %v, %v", h.Id(), *leader)
// return h.Id() == *leader
// })
// case <-timeout:
// close(cancelled)
// return nil
// }
// }
//
// func RemoveHost(cluster []*host, i int) []*host {
// ret := make([]*host, 0, len(cluster)-1)
// ret = append(ret, cluster[:i]...)
// ret = append(ret, cluster[i+1:]...)
// return ret
// }
//
// func SyncMajority(cluster []*host, fn func(h *host) bool) {
// done := make(map[uuid.UUID]struct{})
// start := time.Now()
//
// majority := majority(len(cluster))
// for len(done) < majority {
// for _, r := range cluster {
// id := r.core.replica.Id
// if _, ok := done[id]; ok {
// continue
// }
//
// if fn(r) {
// done[id] = struct{}{}
// continue
// }
//
// if time.Now().Sub(start) > 10*time.Second {
// r.core.replica.Logger.Info("Still not sync'ed")
// }
// }
// <-time.After(250 * time.Millisecond)
// }
// }
//
// func SyncAll(cluster []*host, fn func(h *host) bool) {
// done := make(map[uuid.UUID]struct{})
// start := time.Now()
//
// for len(done) < len(cluster) {
// for _, r := range cluster {
// id := r.core.replica.Id
// if _, ok := done[id]; ok {
// continue
// }
//
// if fn(r) {
// done[id] = struct{}{}
// continue
// }
//
// if time.Now().Sub(start) > 10*time.Second {
// r.core.replica.Logger.Info("Still not sync'ed")
// }
// }
// <-time.After(250 * time.Millisecond)
// }
// }
//
// func First(cluster []*host, fn func(h *host) bool) *host {
// for _, h := range cluster {
// if fn(h) {
// return h
// }
// }
//
// return nil
// }
//
// func Index(cluster []*host, fn func(h *host) bool) int {
// for i, h := range cluster {
// if fn(h) {
// return i
// }
// }
//
// return -1
// }
