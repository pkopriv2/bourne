package elmer

import (
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/stash"
	"github.com/stretchr/testify/assert"
)

func TestIndexer(t *testing.T) {
	conf := common.NewConfig(map[string]interface{}{
		"bourne.log.level": int(common.Debug),
	})

	ctx := common.NewContext(conf)
	defer ctx.Close()

	timer := ctx.Timer(60*time.Second)
	defer timer.Close()

	indexer, err := newTestIndexer(ctx, timer.Closed())
	if err != nil {
		t.Fail()
		return
	}

	t.Run("StoreEnable_Canceled", func(t *testing.T) {
		timer := ctx.Timer(time.Nanosecond)
		defer timer.Close()

		_, _, err := indexer.StoreEnable(timer.Closed(), emptyPath.Child([]byte{0}, 0))
		assert.Equal(t, common.CanceledError, common.Extract(err, common.CanceledError))
	})

	t.Run("StoreEnable_First_BadVersion", func(t *testing.T) {
		_, ok, err := indexer.StoreEnable(timer.Closed(), emptyPath.Child([]byte{0}, 0))
		assert.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("StoreEnable_First", func(t *testing.T) {
		path := emptyPath.Child([]byte{0}, -1)
		info, ok, err := indexer.StoreEnable(timer.Closed(), path)
		assert.Nil(t, err)
		assert.True(t, ok)

		expected := storeInfo{path.Parent().Child([]byte{0}, 0), true}
		assert.Equal(t, expected, info)
	})

	t.Run("StoreEnable_AlreadyEnabled", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreEnable_AlreadyEnabled"), -1)

		child, ok, err := indexer.StoreEnable(timer.Closed(), path)
		assert.Nil(t, err)
		assert.True(t, ok)

		_, ok, err = indexer.StoreEnable(timer.Closed(), child.Path)
		assert.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("StoreEnable_PathError", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreEnable_PathError"), -1).Child([]byte{0}, -1)

		_, _, err := indexer.StoreEnable(timer.Closed(), path)
		assert.Equal(t, PathError, common.Extract(err, PathError))
	})

	t.Run("StoreDisable_NoExist", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreDisable_NoExist"), 0)

		_, ok, err := indexer.StoreDisable(timer.Closed(), path)
		assert.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("StoreDisable", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreDisable"), -1)

		enabled, ok, err := indexer.StoreEnable(timer.Closed(), path)
		assert.Nil(t, err)
		assert.True(t, ok)

		disabled, ok, err := indexer.StoreDisable(timer.Closed(), enabled.Path)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, storeInfo{enabled.Path.Parent().Child(enabled.Path.Last().Elem, 1), false}, disabled)
	})

	t.Run("StoreDisable_AlreadyDisabled", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreDisable_AlreadyDisabled"), -1)

		enabled, ok, err := indexer.StoreEnable(timer.Closed(), path)
		assert.Nil(t, err)
		assert.True(t, ok)

		disabled, ok, err := indexer.StoreDisable(timer.Closed(), enabled.Path)
		assert.Nil(t, err)
		assert.True(t, ok)

		_, ok, err = indexer.StoreDisable(timer.Closed(), disabled.Path)
		assert.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("StoreInfo_NoExist", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreInfo_NoExist"), 0)

		_, ok, err := indexer.StoreInfo(timer.Closed(), path.Parent(), path.Last().Elem)
		assert.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("StoreInfo", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreInfo"), -1)

		enabled, ok, err := indexer.StoreEnable(timer.Closed(), path)
		assert.Nil(t, err)
		assert.True(t, ok)

		info, ok, err := indexer.StoreInfo(timer.Closed(), path.Parent(), path.Last().Elem)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, storeInfo{enabled.Path, true}, info)
	})

	t.Run("StoreItemRead_PathError", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreItemRead_StoreNoExist"), -1)

		_, _, err := indexer.StoreItemRead(timer.Closed(), path, []byte("key"))
		assert.Equal(t, PathError, common.Extract(err, PathError))
	})

	t.Run("StoreItemRead_NoExist", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreItemRead_NoExist"), -1)

		info, ok, err := indexer.StoreEnable(timer.Closed(), path)
		assert.Nil(t, err)
		assert.True(t, ok)

		_, ok, err = indexer.StoreItemRead(timer.Closed(), info.Path, []byte("key"))
		assert.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("StoreItemSwapAndRead", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreItemSwapAndRead"), -1)

		info, ok, err := indexer.StoreEnable(timer.Closed(), path)
		assert.Nil(t, err)
		assert.True(t, ok)

		_, ok, err = indexer.StoreItemSwap(timer.Closed(), info.Path, []byte("key"), []byte("val"), -1, false)
		assert.Nil(t, err)
		assert.True(t, ok)

		_, ok, err = indexer.StoreItemRead(timer.Closed(), info.Path, []byte("key"))
		assert.Nil(t, err)
		assert.True(t, ok)
	})

	t.Run("StoreItemSwap_Atomicity", func(t *testing.T) {
		path := emptyPath.Child([]byte("StoreItemSwap_Atomicity"), -1)

		info, ok, err := indexer.StoreEnable(timer.Closed(), path)
		assert.Nil(t, err)
		assert.True(t, ok)

		numRoutines := 10
		numIncPerRoutine := 100

		var wait sync.WaitGroup
		for i := 0; i < numRoutines; i++ {
			wait.Add(1)
			go func(i int) {
				defer wait.Done()

				err = indexerIncrementN(timer.Closed(), indexer, info.Path, []byte("key"), numIncPerRoutine)
				if err != nil {
					ctx.Logger().Error("ERROR: %+v", err)
					return
				}
			}(i)
		}

		wait.Wait()
		val, _, err := indexerReadInt(timer.Closed(), indexer, info.Path, []byte("key"))
		assert.Nil(t, err)
		assert.Equal(t, numRoutines*numIncPerRoutine, val)
	})
}

func newTestIndexer(ctx common.Context, cancel <-chan struct{}) (*indexer, error) {
	raw, err := kayak.StartTestHost(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	leader := kayak.ElectLeader(cancel, []kayak.Host{raw})
	if leader == nil || common.IsClosed(cancel) {
		return nil, errors.WithStack(common.CanceledError)
	}

	indexer, err := newIndexer(ctx, raw, 10)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return indexer, nil
}

func indexerIncrementN(cancel <-chan struct{}, indexer *indexer, path path, key []byte, n int) error {
	for i := 0; i < n; i++ {
		for {
			ok, err := indexerIncrementInt(cancel, indexer, path, key)
			if err != nil {
				return errors.WithStack(err)
			}
			if ok {
				break
			}
		}
	}
	return nil
}

func indexerIncrementInt(cancel <-chan struct{}, indexer *indexer, path path, key []byte) (bool, error) {
	val, ver, err := indexerReadInt(cancel, indexer, path, key)
	if err != nil {
		return false, errors.WithStack(err)
	}

	_, ok, err := indexerSwapInt(cancel, indexer, path, key, val+1, ver)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return ok, nil
}

func indexerReadInt(cancel <-chan struct{}, indexer *indexer, path path, key []byte) (int, int, error) {
	item, ok, err := indexer.StoreItemRead(cancel, path, key)
	if err != nil {
		return 0, -1, errors.WithStack(err)
	}

	if !ok {
		return 0, -1, nil
	}

	val, err := stash.ParseInt(item.Val)
	if err != nil {
		return 0, -1, errors.WithStack(err)
	}

	return val, item.Ver, nil
}

func indexerSwapInt(cancel <-chan struct{}, indexer *indexer, path path, key []byte, val int, ver int) (int, bool, error) {
	item, ok, err := indexer.StoreItemSwap(cancel, path, key, stash.IntBytes(val), ver, false)
	if err != nil {
		return -1, false, errors.WithStack(err)
	}

	if !ok {
		return -1, false, nil
	}

	return item.Ver, true, nil
}
