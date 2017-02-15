package elmer

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/stretchr/testify/assert"
)

func TestIndexer_SmokeTest(t *testing.T) {

	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	timer := ctx.Timer(30*time.Second)
	defer timer.Close()

	raw, err := kayak.StartTestHost(ctx)
	assert.Nil(t, err)

	indexer, err := newIndexer(ctx, raw, 1)
	assert.Nil(t, err)

	_, ok, err := indexer.ReadItem(timer.Closed(), []byte("store"), []byte("key"))
	assert.Nil(t, err)
	assert.False(t, ok)
}
