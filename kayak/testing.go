package kayak

// Transienting utilities for dependent projects...makes it easier to stand up local
// clusters, etc...

// func StartTransientPeer(ctx common.Context, addr string) Peer {
// stash, err := stash.OpenTransient(ctx)
// if err != nil {
// panic(err)
// }
//
// host, err := Start(ctx, stash.Path(), port, seedPort)
// if err != nil {
// panic(err)
// }
//
// return host
// }
//
// func StartTransientCluster(ctx common.Context, size int) []Peer {
// db := OpenTestLogStash(ctx)
//
// hosts := make([]Peer, 0, size)
// for i := 0; i < size; i++ {
// host, err := newPeer(ctx, net.NewTcpNetwork(), NewBoltStore(db), db, ":0")
// if err != nil {
// panic(err)
// }
//
// hosts = append(hosts, host)
// ctx.Control().Defer(func(error) {
// host.Close()
// })
// }
//
// zero := hosts[0]
// zero.Start()
// Converge(hosts[:1])
//
// for i, h := range hosts[1:] {
// ctx.Logger().Info("Adding host [%v]", h.core.Self)
// if err := h.Join(zero.core.Self.Addr); err != nil {
// panic(err)
// }
//
// SyncAll(hosts[:i+2], func(h Peer) bool {
// return hasPeer(zero.core.Cluster(), h.core.Self) && equalPeers(zero.core.Cluster(), h.core.Cluster())
// })
// }
//
// return hosts
// }
//
// func Converge(cluster []Peer) Peer {
// var term int = 0
// var leader *uuid.UUID
//
// cancelled := make(chan struct{})
// done, timeout := concurrent.NewBreaker(30*time.Second, func() {
// SyncAll(cluster, func(h Peer) bool {
// select {
// case <-cancelled:
// return true
// default:
// }
//
// copy := h.core.CurrentTerm()
// if copy.Num > term {
// term = copy.Num
// }
//
// if copy.Num == term && copy.Leader != nil {
// leader = copy.Leader
// }
//
// return leader != nil && copy.Leader == leader && copy.Num == term
// })
// })
//
// // data race... ?
// select {
// case <-done:
// return First(cluster, func(h Peer) bool {
// return h.Id() == *leader
// })
// case <-timeout:
// close(cancelled)
// return nil
// }
// }
//
// func RemovePeer(cluster []Peer, i int) []Peer {
// ret := make([]Peer, 0, len(cluster)-1)
// ret = append(ret, cluster[:i]...)
// ret = append(ret, cluster[i+1:]...)
// return ret
// }
//
// func SyncMajority(cluster []Peer, fn func(h Peer) bool) {
// done := make(map[uuid.UUID]struct{})
// start := time.Now()
//
// majority := majority(len(cluster))
// for len(done) < majority {
// for _, r := range cluster {
// id := r.core.Id
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
// r.core.logger.Info("Still not sync'ed")
// }
// }
// <-time.After(250 * time.Millisecond)
// }
// }
//
// func SyncAll(cluster []Peer, fn func(h Peer) bool) {
// done := make(map[uuid.UUID]struct{})
// start := time.Now()
//
// for len(done) < len(cluster) {
// for _, r := range cluster {
// id := r.core.Id
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
// r.core.logger.Info("Still not sync'ed")
// }
// }
// <-time.After(250 * time.Millisecond)
// }
// }
//
// func First(cluster []Peer, fn func(h Peer) bool) Peer {
// for _, h := range cluster {
// if fn(h) {
// return h
// }
// }
//
// return nil
// }
//
// func Index(cluster []Peer, fn func(h Peer) bool) int {
// for i, h := range cluster {
// if fn(h) {
// return i
// }
// }
//
// return -1
// }
