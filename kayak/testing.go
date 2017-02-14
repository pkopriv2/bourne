package kayak

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

// Transienting utilities for dependent projects...makes it easier to stand up local
// clusters, etc...

func TestStorage(ctx common.Context) (func(*Options), error) {
	db, err := stash.OpenTransient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Error opening transient storage")
	}
	return func(o *Options) { o.WithStorage(db) }, nil
}

func TestCluster(ctx common.Context, size int) (peers []Host, err error) {
	if size < 1 {
		return []Host{}, nil
	}

	ctx = ctx.Sub("Cluster(size=%v)", size)
	defer func() {
		if err != nil {
			ctx.Control().Close()
		}
	}()

	stg, err := TestStorage(ctx)
	if err != nil {
		return nil, err
	}

	// start the first
	first, err := Start(ctx, ":0", stg)
	if err != nil {
		return nil, errors.Wrap(err, "Error starting first host")
	}
	ctx.Control().Defer(func(error) {
		first.Close()
	})

	first = Converge(ctx.Control().Closed(), []Host{first})
	if first == nil {
		return nil, errors.Wrap(NoLeaderError, "First member failed to become leader")
	}

	hosts := []Host{first}
	for i := 1; i < size; i++ {
		host, err := Join(ctx, ":0", first.Addrs(), stg)
		if err != nil {
			return nil, errors.Wrapf(err, "Error starting [%v] host", i)
		}

		hosts = append(hosts, host)
		ctx.Control().Defer(func(error) {
			host.Close()
		})
	}

	return hosts, nil
}

func Converge(cancel <-chan struct{}, cluster []Host) Host {
	var term int = 0
	var leader *uuid.UUID

	SyncMajority(cancel, cluster, func(h Host) bool {
		copy := h.(*host).core.CurrentTerm()
		if copy.Num > term {
			term = copy.Num
		}

		if copy.Num == term && copy.Leader != nil {
			leader = copy.Leader
		}

		return leader != nil && copy.Leader == leader && copy.Num == term
	})

	if leader == nil || common.IsCanceled(cancel) {
		return nil
	}

	return First(cluster, func(h Host) bool {
		return h.Id() == *leader
	})
}

func SyncMajority(cancel <-chan struct{}, cluster []Host, fn func(h Host) bool) {
	done := make(map[uuid.UUID]struct{})
	start := time.Now()

	majority := majority(len(cluster))
	for len(done) < majority {
		for _, h := range cluster {
			if common.IsCanceled(cancel) {
				return
			}

			if _, ok := done[h.Id()]; ok {
				continue
			}

			if fn(h) {
				done[h.Id()] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				h.Context().Logger().Info("Still not sync'ed")
			}
		}
		<-time.After(250 * time.Millisecond)
	}
}

func SyncAll(cancel <-chan struct{}, cluster []Host, fn func(h Host) bool) {
	done := make(map[uuid.UUID]struct{})
	start := time.Now()

	for len(done) < len(cluster) {
		for _, h := range cluster {
			if common.IsCanceled(cancel) {
				return
			}

			if _, ok := done[h.Id()]; ok {
				continue
			}

			if fn(h) {
				done[h.Id()] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				h.Context().Logger().Info("Still not sync'ed")
			}
		}
		<-time.After(250 * time.Millisecond)
	}
}

func First(cluster []Host, fn func(h Host) bool) Host {
	for _, h := range cluster {
		if fn(h) {
			return h
		}
	}

	return nil
}

func Index(cluster []Host, fn func(h Host) bool) int {
	for i, h := range cluster {
		if fn(h) {
			return i
		}
	}

	return -1
}
