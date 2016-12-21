package kayak

// func TestMember_Close(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Error),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// logger := ctx.Logger().Fmt("TEST: ")
//
// go func() {
// tick := time.NewTicker(200 * time.Millisecond)
// for range tick.C {
// ctx.Logger().Error("#Routines: %v", runtime.NumGoroutine())
// }
// }()
//
// failures := make([]string, 0, 1000)
// for i := 0; i < 100; i++ {
// success, msg := RunClusterTest(11 + rand.Intn(23))
// if !success {
// failures = append(failures, msg)
// }
// }
//
// for _, f := range failures {
// logger.Error("Error: %v", f)
// }
//
// assert.Empty(t, failures)
// }
//
// func RunClusterTest(size int) (bool, string) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Info),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// cluster := StartKayakCluster(ctx, convoy.StartTransientCluster(ctx, 9290, size), 9390)
// indices := rand.Perm(size)
// killed := rand.Intn((size / 2))
// // logger := ctx.Logger().Fmt("TEST[%v,%v]", size, killed)
//
// // logger.Error("Starting cluster")
// _, _, ok1 := Converge(cluster)
// if !ok1 {
// return false, fmt.Sprintf("Unable to converge cluster of [%v]", size)
// }
//
// // logger.Error("Killing hosts: %v", indices[:killed])
// for i := 0; i < killed; i++ {
// cluster[indices[i]].Close()
// }
//
// // logger.Error("Converging remaining hosts: %v", indices[killed:])
// remaining := make([]*host, 0, len(cluster))
// for i := killed; i < size; i++ {
// remaining = append(remaining, cluster[indices[i]])
// }
//
// _, _, ok2 := Converge(remaining)
// if !ok2 {
// return false, fmt.Sprintf("Unable to converge cluster after removing hosts: %v", indices[:killed])
// }
//
// return true, ""
// }
//
// func StartKayakCluster(ctx common.Context, cluster []convoy.Host, start int) []*host {
// peers := make([]peer, 0, len(cluster))
// for i, h := range cluster {
// m, err := h.Self()
// if err != nil {
// panic(err)
// }
//
// peers = append(peers, peer{raw: m, port: start + i})
// }
//
// ctx.Logger().Info("Starting kayak cluster [%v]", peers)
//
// hosts := make([]*host, 0, len(peers))
// for i, p := range peers {
// others := make([]peer, 0, len(peers)-1)
// others = append(others, peers[:i]...)
// others = append(others, peers[i+1:]...)
//
// host, err := newHost(ctx, p, others)
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
// func Converge(cluster []*host) (int, *uuid.UUID, bool) {
// var term int = 0
// var leader *uuid.UUID
//
// cancelled := make(chan struct{})
// done, timeout := concurrent.NewBreaker(10*time.Second, func() interface{} {
// SyncCluster(cluster, func(h *host) bool {
// select {
// case <-cancelled:
// return true
// default:
// }
//
// copy := h.member.snapshot()
// if copy.term > term {
// term = copy.term
// }
//
// if copy.term == term && copy.leader != nil {
// leader = copy.leader
// }
//
// return leader != nil && copy.leader == leader && copy.term == term
// })
// return nil
// })
//
// select {
// case <-done:
// return term, leader, true
// case <-timeout:
// close(cancelled)
// return term, leader, false
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
// func SyncCluster(cluster []*host, fn func(h *host) bool) {
// done := make(map[uuid.UUID]struct{})
// start := time.Now()
//
// cluster[0].member.logger.Info("Syncing cluster [%v]", len(cluster))
//
// for len(done) < len(cluster) {
// cluster[0].member.logger.Info("Number of sync'ed: %v", len(done))
// for _, r := range cluster {
// id := r.member.self.raw.Id()
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
// r.member.logger.Info("Still not sync'ed")
// }
// }
// <-time.After(250 * time.Millisecond)
// }
// cluster[0].member.logger.Info("Number of sync'ed: %v", len(done))
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
