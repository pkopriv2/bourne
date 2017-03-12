package elmer

// Transienting utilities for dependent projects...makes it easier to stand up local
// clusters, etc...

// func NewTestCluster(ctx common.Context, num int) ([]Peer, error) {
// cluster, err := kayak.StartTestCluster(ctx, num)
// if err != nil {
// return nil, errors.WithStack(err)
// }
//
// timer := ctx.Timer(30 * time.Second)
// defer timer.Close()
//
// kayak.ElectLeader(timer.Closed(), cluster)
//
// ret := make([]Peer, 0, len(cluster))
// for _, host := range cluster {
// peer, err := NewTestPeer(ctx, host)
// if err != nil {
// return nil, errors.WithStack(err)
// }
// ret = append(ret, peer)
// }
// return ret, nil
// }
//
// func NewTestPeer(ctx common.Context, host kayak.Host) (*peer, error) {
// p, err := Start(ctx, host, ":0")
// if err != nil {
// return nil, errors.WithStack(err)
// }
// return p.(*peer), nil
// }
