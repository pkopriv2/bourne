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

	StartKayakCluster(ctx, convoy.StartTransientCluster(ctx, 9290, 3), 9390)
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
