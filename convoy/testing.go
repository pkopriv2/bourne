package convoy

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

// Transienting utilities for dependent projects...makes it easier to stand up local
// clusters, etc...

func StartTransientSeedHost(ctx common.Context, port int) Host {
	stash, err := stash.OpenTransient(ctx)
	if err != nil {
		panic(err)
	}

	host, err := StartSeedHost(ctx, stash.Path(), port)
	if err != nil {
		panic(err)
	}

	return host
}

func StartTransientHost(ctx common.Context, port int, seedPort int) Host {
	stash, err := stash.OpenTransient(ctx)
	if err != nil {
		panic(err)
	}

	host, err := StartHost(ctx, stash.Path(), port, seedPort)
	if err != nil {
		panic(err)
	}

	return host
}

func StartTransientCluster(ctx common.Context, start int, num int) []Host {
	seeder := StartTransientSeedHost(ctx, start)

	cluster := []Host{seeder}
	for i := num - 1; i > 0; i-- {
		h := StartTransientHost(ctx, start+i, start)
		cluster = append(cluster, h)
	}

	SyncCluster(cluster, func(h Host) bool {
		all, err := h.Directory().All()
		if err != nil {
			panic(err)
		}

		return len(all) == len(cluster)
	})

	seeder.(*host).logger.Info("Successfully started cluster of [%v] hosts", len(cluster))
	return cluster
}

func SyncCluster(cluster []Host, fn func(r Host) bool) {
	done := make(map[uuid.UUID]struct{})
	start := time.Now()

	cluster[0].(*host).logger.Info("Syncing cluster [%v]", len(cluster))
	for len(done) < len(cluster) {
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
