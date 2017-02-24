package elmer

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func TestRoster_Get_Empty(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	roster, err := newTestRoster(ctx, timer.Closed())
	if err != nil {
		t.Fail()
		return
	}

	peers, err := roster.Get(timer.Closed())
	assert.Nil(t, err)
	assert.Equal(t, []string{}, peers)
}

func TestRoster_Get_NonEmpty(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	roster, err := newTestRoster(ctx, timer.Closed())
	if err != nil {
		t.Fail()
		return
	}

	added, err := roster.Add(timer.Closed(), "peer")
	assert.Nil(t, err)

	read, err := roster.Get(timer.Closed())
	assert.Nil(t, err)
	assert.Equal(t, added, read)
}

func TestRoster_Add_Empty(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	roster, err := newTestRoster(ctx, timer.Closed())
	if err != nil {
		t.Fail()
		return
	}

	peers, err := roster.Add(timer.Closed(), "peer")
	assert.Nil(t, err)
	assert.Equal(t, []string{"peer"}, peers)
}

func TestRoster_Add_NonEmpty(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	roster, err := newTestRoster(ctx, timer.Closed())
	if err != nil {
		t.Fail()
		return
	}

	_, err = roster.Add(timer.Closed(), "peer1")
	assert.Nil(t, err)

	peers, err := roster.Add(timer.Closed(), "peer2")
	assert.Nil(t, err)
	assert.Equal(t, []string{"peer1", "peer2"}, peers)
}

func TestRoster_Add_NotUnique(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	timer := ctx.Timer(30 * time.Second)
	defer timer.Close()

	roster, err := newTestRoster(ctx, timer.Closed())
	if err != nil {
		t.Fail()
		return
	}

	_, err = roster.Add(timer.Closed(), "peer1")
	assert.Nil(t, err)

	peers, err := roster.Add(timer.Closed(), "peer1")
	assert.Nil(t, err)
	assert.Equal(t, []string{"peer1"}, peers)
}

func newTestRoster(ctx common.Context, cancel <-chan struct{}) (*roster, error) {
	indexer, err := newTestIndexer(ctx, cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return newRoster(indexer), nil
}
