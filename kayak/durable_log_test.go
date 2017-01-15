package kayak

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	"github.com/stretchr/testify/assert"
)

func OpenTestLogStash(ctx common.Context) stash.Stash {
	db := OpenTestStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		return initBuckets(tx)
	})
	return db
}

func OpenTestDurableLog(ctx common.Context) durableLog {
	return durableLog{}
}

func TestDurableLog_CreateSnapshot_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		s, err := createDurableSnapshot(tx, []Event{}, []byte{})
		if err != nil {
			panic(err)
		}

		snapshot, ok, err := openDurableSnapshot(tx, s.id)
		if err != nil {
			panic(err)
		}

		assert.True(t, ok)
		assert.Equal(t, s.id, snapshot.id)
		assert.Equal(t, s.Config(), snapshot.Config())

		events, err := snapshot.Events(tx)
		if err != nil {
			panic(err)
		}

		assert.Equal(t, []Event{}, events)
		return nil
	})
}

func TestDurableLog_CreateSnapshot_Config(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected := []byte{0,1,2}

	db := OpenTestLogStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		snapshot, err := createDurableSnapshot(tx, []Event{}, expected)
		if err != nil {
			panic(err)
		}

		assert.NotNil(t, snapshot.id)
		assert.Equal(t, expected, snapshot.Config())
		return nil
	})
}

func TestDurableLog_CreateSnapshot_Events(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected := []Event{[]byte{0,1},[]byte{0,1}}

	db := OpenTestLogStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		snapshot, err := createDurableSnapshot(tx, expected, []byte{})
		if err != nil {
			panic(err)
		}

		assert.NotNil(t, snapshot.id)
		assert.Equal(t, []byte{}, snapshot.Config())

		events, err := snapshot.Events(tx)
		if err != nil {
			panic(err)
		}

		assert.Equal(t, expected, events)
		return nil
	})
}

func TestDurableLog_CreateSnapshot_MultipleWithEvents(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected1 := []Event{[]byte{0,1},[]byte{2,3}}
	expected2 := []Event{[]byte{0,1,2},[]byte{3},[]byte{4,5}}

	db := OpenTestLogStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		snapshot1, err := createDurableSnapshot(tx, expected1, []byte{})
		if err != nil {
			panic(err)
		}

		snapshot2, err := createDurableSnapshot(tx, expected2, []byte{})
		if err != nil {
			panic(err)
		}

		events1, err := snapshot1.Events(tx)
		if err != nil {
			panic(err)
		}

		events2, err := snapshot2.Events(tx)
		if err != nil {
			panic(err)
		}

		assert.Equal(t, expected1, events1)
		assert.Equal(t, expected2, events2)
		return nil
	})
}
