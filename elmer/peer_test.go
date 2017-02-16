package elmer

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
	"github.com/stretchr/testify/assert"
)

func TestPeer_Start(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	timer := ctx.Timer(10 * time.Second)
	defer timer.Close()

	raw, err := kayak.StartTestHost(ctx)
	assert.Nil(t, err)

	kayak.ElectLeader(timer.Closed(), []kayak.Host{raw})

	_, err = newPeer(ctx, raw, net.NewTcpNetwork(), ":0")
	assert.Nil(t, err)
}
