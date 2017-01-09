package amoeba

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex_Get_NoExist_Bytes(t *testing.T) {
	index := NewBTreeIndex(32)
	assert.Nil(t, Get(index, BytesKey([]byte{0})))
}

func TestIndex_Get_Exist_Bytes(t *testing.T) {
	index := NewBTreeIndex(32)
	Put(index, BytesKey([]byte{0}), "val")
	assert.Equal(t, "val", Get(index, BytesKey([]byte{0})))
}

func TestIndex_Get_NoExist(t *testing.T) {
	index := NewBTreeIndex(32)
	assert.Nil(t, Get(index, IntKey(1)))
}

func TestIndex_Get_Exist(t *testing.T) {
	index := NewBTreeIndex(32)
	Put(index, IntKey(1), "val")
	assert.Equal(t, "val", Get(index, IntKey(1)))
}

func TestIndex_Get_Deleted(t *testing.T) {
	index := NewBTreeIndex(32)
	Put(index, IntKey(1), "val")
	Del(index, IntKey(1))
	assert.Nil(t, Get(index, IntKey(1)))
}

func TestIndex_Put_Exist(t *testing.T) {
	index := NewBTreeIndex(32)
	Put(index, IntKey(1), "val")
	Put(index, IntKey(1), "val2")
	assert.Equal(t, "val2", Get(index, IntKey(1)))
}

func TestIndex_Del_NoExist(t *testing.T) {
	index := NewBTreeIndex(32)
	Del(index, IntKey(1))
	assert.Nil(t, Get(index, IntKey(1)))
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

	Put(index, IntKey(1), "val")

	i := 0
	ScanAll(index, func(s Scan, k Key, v interface{}) {
		assert.Equal(t, IntKey(1), k)
		assert.Equal(t, "val", v)
		i++
	})

	assert.Equal(t, 1, i)
}

func TestIndex_Scan_All(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, IntKey(i), i)
	}

	i := 0
	ScanAll(index, func(s Scan, k Key, v interface{}) {
		assert.Equal(t, IntKey(i), k)
		assert.Equal(t, i, v)
		i++
	})

	assert.Equal(t, 1024, i)
}

func TestIndex_Scan_Stop(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, IntKey(i), i)
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
		Put(index, IntKey(i), i)
	}

	i := 0
	ScanAll(index, func(s Scan, k Key, v interface{}) {
		s.Next(IntKey(1025))
		i++
	})

	assert.Equal(t, 1, i)
}

func TestIndex_Scan_Skip(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, IntKey(i), i)
	}

	i := 0
	ScanAll(index, func(s Scan, k Key, v interface{}) {
		if v.(int)%2 == 0 {
			s.Next(IntKey(v.(int) + 1))
			return
		}

		i++
	})

	assert.Equal(t, 512, i)
}

func TestIndex_Scan_From_GreaterThanMax(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, IntKey(i), i)
	}

	i := 0
	ScanFrom(index, IntKey(1025), func(s Scan, k Key, v interface{}) {
		i++
	})

	assert.Equal(t, 0, i)
}

func TestIndex_Scan_From(t *testing.T) {
	index := NewBTreeIndex(32)

	for i := 0; i < 1024; i++ {
		Put(index, IntKey(i), i)
	}

	i := 0
	ScanFrom(index, IntKey(512), func(s Scan, k Key, v interface{}) {
		i++
	})

	assert.Equal(t, 512, i)
}
