package kayak

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/convoy"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestMember_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	cluster := StartKayakCluster(ctx, convoy.StartTransientCluster(ctx, 9290, 5), 9390)

	time.Sleep(5 * time.Second)

	// var leader *uuid.UUID
	leader1 := First(cluster, func(h *host) bool {
		l := h.member.snapshot().leader
		return l != nil && *l == h.member.id
	})

	assert.NotNil(t, leader1)
	assert.Nil(t, leader1.Close())

	time.Sleep(5 * time.Second)

	// var leader *uuid.UUID
	leader2 := First(cluster, func(h *host) bool {
		l := h.member.snapshot().leader
		return l != nil && *l == h.member.id && *l != leader1.member.id
	})

	assert.NotNil(t, leader2)
	assert.Nil(t, leader2.Close())

	leader2.Close()


	time.Sleep(100 * time.Second)
}

func StartKayakCluster(ctx common.Context, cluster []convoy.Host, start int) []*host {
	peers := make([]peer, 0, len(cluster))
	for i, h := range cluster {
		m, err := h.Self()
		if err != nil {
			panic(err)
		}

		peers = append(peers, peer{raw: m, port: start + i})
	}

	ctx.Logger().Info("Starting kayak cluster [%v]", peers)

	hosts := make([]*host, 0, len(peers))
	for i, p := range peers {
		others := make([]peer, 0, len(peers)-1)
		others = append(others, peers[:i]...)
		others = append(others, peers[i+1:]...)

		host, err := newHost(ctx, p, others)
		if err != nil {
			panic(err)
		}

		hosts = append(hosts, host)
		ctx.Env().OnClose(func() {
			host.Close()
		})
	}

	return hosts
}

func SyncCluster(cluster []*host, fn func(h *host) bool) {
	done := make(map[uuid.UUID]struct{})
	start := time.Now()

	cluster[0].member.logger.Info("Syncing cluster [%v]", len(cluster))

	for len(done) < len(cluster) {
		cluster[0].member.logger.Info("Number of sync'ed: %v", len(done))
		for _, r := range cluster {
			id := r.member.self.raw.Id()
			if _, ok := done[id]; ok {
				continue
			}

			if fn(r) {
				done[id] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				r.member.logger.Info("Still not sync'ed")
			}
		}
		<-time.After(250 * time.Millisecond)
	}
	cluster[0].member.logger.Info("Number of sync'ed: %v", len(done))
}

func First(cluster []*host, fn func(h *host) bool) *host {
	for _, h := range cluster {
		if fn(h) {
			return h
		}
	}

	return nil
}
