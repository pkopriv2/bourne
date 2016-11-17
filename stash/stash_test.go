package stash

import (
	"encoding/binary"
	"path"
	"sync"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

var TestSuffix = uuid.NewV4().String()

func GetTestPath(dir string, file string) string {
	return path.Join(afero.GetTempDir(afero.NewOsFs(), dir), file)
}

func TestStash_Close(t *testing.T) {
	ctx := common.NewContext(
		common.NewConfig(
			map[string]interface{}{
				StashLocationKey: GetTestPath(TestSuffix, "db")}))

	stash1, err1 := Open(ctx)
	assert.Nil(t, err1)

	ctx.Close()
	assert.NotNil(t, stash1.Update(func(*bolt.Tx) error {
		return nil
	}))
}

func TestStash_ConcurrentUpdates(t *testing.T) {
	ctx := common.NewContext(
		common.NewConfig(
			map[string]interface{}{
				StashLocationKey: GetTestPath(TestSuffix, "db")}))
	defer ctx.Close()

	stash, err := Open(ctx)
	assert.Nil(t, err)

	bucket := []byte("bucket")
	key := []byte("counter")

	get := func(stash Stash) (counter uint64) {
		stash.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(bucket)
			if bucket == nil {
				counter = 0
				return nil
			}

			bytes := bucket.Get([]byte(key))
			if bytes == nil {
				counter = 0
				return nil
			}

			counter = BytesToUint64(bytes)
			return nil
		})
		return
	}

	inc := func(stash Stash) {
		err := stash.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists(bucket)
			if err != nil {
				panic(err)
			}

			var val uint64

			bytes := bucket.Get([]byte(key))
			if bytes != nil {
				val = BytesToUint64(bytes)
			}

			return bucket.Put([]byte(key), Uint64ToBytes(val+1))
		})

		if err != nil {
			panic(err)
		}
	}

	var wait sync.WaitGroup

	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < 100; i++ {
			inc(stash)
		}
	}()

	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < 100; i++ {
			inc(stash)
		}
	}()

	wait.Wait()
	assert.Equal(t, uint64(200), get(stash))
}

func TestStash_SameLocation(t *testing.T) {
	ctx := common.NewContext(
		common.NewConfig(
			map[string]interface{}{
				StashLocationKey: GetTestPath(TestSuffix, "db")}))
	defer ctx.Close()

	stash1, err1 := Open(ctx)
	assert.Nil(t, err1)

	stash2, err2 := Open(ctx)
	assert.Nil(t, err2)

	assert.Equal(t, stash1, stash2)
}

// Helper functions
func Uint64ToBytes(id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return key
}

func BytesToUint64(val []byte) uint64 {
	return binary.BigEndian.Uint64(val)
}
