package convoy

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// Transienting utilities for dependent projects...makes it easier to stand up local
// clusters, etc...

func StartTestHost(ctx common.Context, addr string) (Host, error) {

	host, err := newHost(ctx, net.NewTcpNetwork(), addr, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return host, nil
}

func JoinTestHost(ctx common.Context, addr string, peers []string) (Host, error) {

	host, err := newHost(ctx, net.NewTcpNetwork(), addr, peers)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return host, nil
}

func StartTestCluster(ctx common.Context, num int) ([]Host, error) {
	ctx = ctx.Sub("TestCluster(%v)", num)

	var err error
	defer func() {
		if err != nil {
			ctx.Control().Fail(err)
		}
	}()

	ctx.Logger().Info("Starting seed host")
	seeder, err := StartTestHost(ctx, ":0")
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ctx.Control().Defer(func(error) {
		seeder.Shutdown()
	})

	member, err := seeder.Self()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cluster := []Host{seeder}
	for i := 1; i < num; i++ {
		var cur Host

		ctx.Logger().Info("Starting test host [%v]", i)
		cur, err = JoinTestHost(ctx, ":0", []string{member.Addr()})
		if err != nil {
			return nil, errors.WithStack(err)
		}

		cluster = append(cluster, cur)
	}

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	SyncCluster(timer.Closed(), cluster, func(h Host) bool {
		dir, err := h.Directory()
		if err != nil {
			return false
		}

		all, err := dir.AllMembers(timer.Closed())
		if err != nil {
			return false
		}

		return len(all) == len(cluster)
	})

	ctx.Control().Defer(func(error) {
		for _, h := range cluster {
			h.Shutdown()
		}
	})

	ctx.Logger().Info("Successfully started cluster of [%v] hosts", len(cluster))
	return cluster, nil
}

func SyncCluster(cancel <-chan struct{}, cluster []Host, fn func(r Host) bool) {
	done := make(map[uuid.UUID]struct{})
	start := time.Now()

	cluster[0].(*host).logger.Info("Syncing cluster [%v]", len(cluster))
	for len(done) < len(cluster) && !common.IsCanceled(cancel) {
		cluster[0].(*host).logger.Info("Number of sync'ed: %v", len(done))
		for _, r := range cluster {
			id := r.Id()
			if _, ok := done[id]; ok {
				continue
			}

			if fn(r) {
				done[id] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				r.(*host).logger.Info("Still not sync'ed")
			}
		}
		<-time.After(250 * time.Millisecond)
	}
	cluster[0].(*host).logger.Info("Number of sync'ed: %v", len(done))
}
