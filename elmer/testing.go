package elmer

import "github.com/pkopriv2/bourne/common"

// Transienting utilities for dependent projects...makes it easier to stand up local
// clusters, etc...

func TestCluster(ctx common.Context, size int) (peers []Peer, err error) {
	if size < 1 {
		return []Peer{}, nil
	}
	return nil, nil

	// ctx = ctx.Sub("Cluster(size=%v)", size)
	// defer func() {
	// if err != nil {
	// ctx.Control().Close()
	// }
	// }()
	//
	// deps, err := NewTransientDependencies(ctx)
	// if err != nil {
	// return nil, errors.Wrap(err, "Error initializing transient dependencies")
	// }
	//
	// // start the first
	// first, err := Start(ctx, deps, ":0")
	// if err != nil {
	// return nil, errors.Wrap(err, "Error starting first host")
	// }
	// ctx.Control().Defer(func(error) {
	// first.Close()
	// })
	//
	// first = Converge(ctx.Control().Closed(), []Host{first})
	// if first == nil {
	// return nil, errors.Wrap(NoLeaderError, "First member failed to become leader")
	// }
	//
	// hosts := []Host{first}
	// for i := 1; i < size; i++ {
	// host, err := Join(ctx, deps, ":0", first.Roster())
	// if err != nil {
	// return nil, errors.Wrapf(err, "Error starting [%v] host", i)
	// }
	//
	// hosts = append(hosts, host)
	// ctx.Control().Defer(func(error) {
	// host.Close()
	// })
	// }
	//
	// return hosts, nil
}
