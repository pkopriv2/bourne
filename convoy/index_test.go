package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/btree"
	"github.com/stretchr/testify/assert"
)

type intKey int

func (i intKey) Less(than btree.Item) bool {
	return i < intKey(than.(intKey))
}

func TestIndex_Get_Empty(t *testing.T) {
	index := newIndex()
	assert.Nil(t, index.Get(intKey(1)))
}

func TestIndex_Get_Exists(t *testing.T) {
	index := newIndex()
	index.Put(intKey(1), "val")
	assert.Equal(t, "val", index.Get(intKey(1)))
}

func TestIndex_Get_NoExist(t *testing.T) {
	index := newIndex()
	index.Put(intKey(1), "val")
	assert.Nil(t, index.Get(intKey(2)))
}

func TestIndex_Remove_NoExist(t *testing.T) {
	index := newIndex()
	index.Put(intKey(1), "val")
	index.Remove(intKey(2))
	assert.Equal(t, "val", index.Get(intKey(1)))
}

func TestIndex_Remove_Exists(t *testing.T) {
	index := newIndex()
	index.Put(intKey(1), "val")
	index.Remove(intKey(1))
	assert.Nil(t, index.Get(intKey(1)))
}

func TestIndex_Scan_Complete(t *testing.T) {
	index := newIndex()
	for i := 0; i < 1024; i++ {
		index.Put(intKey(i), "val")
	}

	i := 0
	index.Scan(func(scan *indexScan, key indexKey) {
		assert.Equal(t, intKey(i), key.(intKey))
		i++
	})

	assert.Equal(t, 1024, i)
}

func TestIndex_Scan_Stop(t *testing.T) {
	index := newIndex()
	for i := 0; i < 1024; i++ {
		index.Put(intKey(i), "val")
	}

	i := 0
	index.Scan(func(scan *indexScan, key indexKey) {
		iter := key.(intKey)
		if iter == 512 {
			scan.Stop()
			return
		}

		i++
	})

	assert.Equal(t, 512, i)
}

func TestIndex_Scan_Skip_GreaterThanMax(t *testing.T) {
	index := newIndex()
	for i := 0; i < 1024; i++ {
		index.Put(intKey(i), "val")
	}

	i := 0
	index.Scan(func(scan *indexScan, key indexKey) {
		scan.Next(intKey(1025))
		i++
	})

	assert.Equal(t, 1, i)
}

func TestIndex_Scan_Skip(t *testing.T) {
	index := newIndex()
	for i := 0; i < 1024; i++ {
		index.Put(intKey(i), "val")
	}

	i := 0
	index.Scan(func(scan *indexScan, key indexKey) {
		iter := key.(intKey)
		if iter == 512 {
			scan.Next(intKey(1023))
			return
		}

		i++
	})

	assert.Equal(t, 513, i)
}
