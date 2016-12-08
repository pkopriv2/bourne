package amoeba

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type intKey int

func (i intKey) Compare(s Key) int {
	return int(i - s.(intKey))
}

func TestIndex_Get_NoExist(t *testing.T) {
	index := NewBTreeIndex(32)
	assert.Nil(t, Get(index, intKey(1)))
}

func TestIndex_Get_Exist(t *testing.T) {
	index := NewBTreeIndex(32)
	Put(index, intKey(1), "val")
	assert.Equal(t, "val", Get(index, intKey(1)))
}

func TestIndex_Get_Deleted(t *testing.T) {
	index := NewBTreeIndex(32)
	Put(index, intKey(1), "val")
	Del(index, intKey(1))
	assert.Nil(t, Get(index, intKey(1)))
}

func TestIndex_Put_Exist(t *testing.T) {
	index := NewBTreeIndex(32)
	Put(index, intKey(1), "val")
	Put(index, intKey(1), "val2")
	assert.Equal(t, "val2", Get(index, intKey(1)))
}

func TestIndex_Del_NoExist(t *testing.T) {
	index := NewBTreeIndex(32)
	Del(index, intKey(1))
	assert.Nil(t, Get(index, intKey(1)))
}

func TestIndex_Scan_Empty(t *testing.T) {
	index := NewBTreeIndex(32)

	i := 0
	ScanAll(index, func(s Scan, k Key, v interface{}) {
		i++
	})

	assert.Equal(t, 0, i)
}

func TestIndex_Scan_Single(t *testing.T) {
	index := NewBTreeIndex(32)

	Put(index, intKey(1), "val")

	i := 0
	ScanAll(index, func(s Scan, k Key, v interface{}) {
		assert.Equal(t, intKey(1), k)
		assert.Equal(t, "val", v)
		i++
	})

	assert.Equal(t, 1, i)
}

func TestIndex_Scan_All(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, intKey(i), i)
	}

	i := 0
	ScanAll(index, func(s Scan, k Key, v interface{}) {
		assert.Equal(t, intKey(i), k)
		assert.Equal(t, i, v)
		i++
	})

	assert.Equal(t, 1024, i)
}

func TestIndex_Scan_Stop(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, intKey(i), i)
	}

	i := 0
	ScanAll(index, func(s Scan, k Key, v interface{}) {
		if i == 512 {
			defer s.Stop()
			return
		}
		i++
	})

	assert.Equal(t, 512, i)
}

func TestIndex_Scan_Skip_GreaterThanMax(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, intKey(i), i)
	}

	i := 0
	ScanAll(index, func(s Scan, k Key, v interface{}) {
		s.Next(intKey(1025))
		i++
	})

	assert.Equal(t, 1, i)
}

func TestIndex_Scan_Skip(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, intKey(i), i)
	}

	i := 0
	ScanAll(index, func(s Scan, k Key, v interface{}) {
		if v.(int)%2 == 0 {
			s.Next(intKey(v.(int) + 1))
			return
		}

		i++
	})

	assert.Equal(t, 512, i)
}

func TestIndex_Scan_From_GreaterThanMax(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, intKey(i), i)
	}

	i := 0
	ScanFrom(index, intKey(1025), func(s Scan, k Key, v interface{}) {
		i++
	})

	assert.Equal(t, 0, i)
}

func TestIndex_Scan_From(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, intKey(i), i)
	}

	i := 0
	ScanFrom(index, intKey(512), func(s Scan, k Key, v interface{}) {
		i++
	})

	assert.Equal(t, 512, i)
}
