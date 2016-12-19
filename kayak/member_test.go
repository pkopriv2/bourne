package kayak

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/convoy"
)

func TestMember_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	StartKayakCluster(ctx, convoy.StartTransientCluster(ctx, 9290, 2), 9390)
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

	members := make([]*host, 0, len(peers))
	for i, p := range peers {
		member, err := newHost(ctx, p, append(peers[:i], peers[i+1:]...))
		if err != nil {
			panic(err)
		}

		members = append(members, member)

		ctx.Env().OnClose(func() {
			member.Close()
		})
	}

	return members
}
