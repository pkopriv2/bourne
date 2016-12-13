package convoy

// TODO: Figure out how to randomize ports!!!

// func TestHost_Close(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// replica := StartTestMasterHost(ctx, 0)
//
// assert.Nil(t, replica.Close())
// assert.NotNil(t, replica.Close())
// }
//
// func TestHost_Leave(t *testing.T) {
// conf := common.NewConfig(map[string]interface{}{
// "bourne.log.level": int(common.Info),
// })
//
// ctx := common.NewContext(conf)
// defer ctx.Close()
//
// size := 64
// host := StartTestMasterHost(ctx, size)
//
// assert.True(t, true)
//
// // //choose a random member
// // idx := rand.Intn(size)
// // rep := cluster[idx]
// // rep.Leave()
// //
// // done, timeout := concurrent.NewBreaker(10*time.Second, func() interface{} {
// // Sync(removeHost(cluster, idx), func(r *replica) bool {
// // return !r.Dir.IsHealthy(rep.Id()) || !r.Dir.IsActive(rep.Id())
// // })
// //
// // return nil
// // })
// //
// // select {
// // case <-done:
// // case <-timeout:
// // t.FailNow()
// // }
//
// // assert.Equal(t, replicaClosedError, rep.ensureOpen())
// }

// func StartTestMasterHost(ctx common.Context, port int) *host {
// return StartTestHostFromDb(ctx, OpenTestDatabase(ctx, OpenTestChangeLog(ctx)), port, "")
// }
//
// func StartTestMemberHost(ctx common.Context, selfPort int, peerPort int) *host {
// return StartTestHostFromDb(ctx, OpenTestDatabase(ctx, OpenTestChangeLog(ctx)), selfPort, net.NewAddr("localhost", strconv.Itoa(peerPort)))
// }
//
// func StartTestHostFromDb(ctx common.Context, db Database, port int, peer string) *host {
// host, err := newMemberHost(ctx, db, "localhost", port, peer)
// if err != nil {
// panic(err)
// }
//
// ctx.Env().OnClose(func() {
// db.Log().(*changeLog).stash.Close()
// })
//
// ctx.Env().OnClose(func() {
// db.Close()
// })
//
// ctx.Env().OnClose(func() {
// host.Close()
// })
//
// return host
// }
