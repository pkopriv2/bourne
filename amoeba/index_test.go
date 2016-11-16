package amoeba

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type intKey int

func (i intKey) Compare(s Sortable) int {
	return int(i - s.(intKey))
}

func NewTestIndex() *index {
	return newIndex(32)
}

func TestIndex_Get_Empty(t *testing.T) {
	index := NewTestIndex()
	assert.Nil(t, index.Get(intKey(1)))
}

func TestIndex_Get_Exists(t *testing.T) {
	index := NewTestIndex()
	item := item{"val", 0, time.Now()}
	index.Put(intKey(1), item.Val(), item.Ver(), item.Time)

	actual := index.Get(intKey(1))
	assert.Equal(t, item, actual)
}

func TestIndex_Get_NoExist(t *testing.T) {
	index := NewTestIndex()

	item := item{"val", 0, time.Now()}
	index.Put(intKey(1), item.Val(), item.Ver(), item.Time)
	assert.Nil(t, index.Get(intKey(2)))
}

func TestIndex_Del_Empty(t *testing.T) {
	index := NewTestIndex()

	item := item{nil, 0, time.Now()}
	index.Del(intKey(1), item.Ver(), item.Time)
	assert.Nil(t, index.Get(intKey(1)))
}

func TestIndex_Del_DelGreaterVersion(t *testing.T) {
	index := NewTestIndex()
	item := item{"val", 0, time.Now()}
	index.Put(intKey(1), item.Val(), item.Ver(), item.Time)
	index.Del(intKey(1), item.Ver()+1, item.Time)
	assert.Nil(t, index.Get(intKey(1)))
}

func TestIndex_Del_DelSameVersion(t *testing.T) {
	index := NewTestIndex()
	item := item{"val", 0, time.Now()}
	index.Put(intKey(1), item.Val(), item.Ver(), item.Time)
	index.Del(intKey(1), item.Ver(), item.Time)
	assert.Nil(t, index.Get(intKey(1)))
}

func TestIndex_Del_DelLessVersion(t *testing.T) {
	index := NewTestIndex()
	item := item{"val", 0, time.Now()}
	index.Put(intKey(1), item.Val(), item.Ver(), item.Time)
	index.Del(intKey(1), item.Ver()-1, item.Time)
	assert.Equal(t, item, index.Get(intKey(1)))
}

func TestIndex_Scan_Complete(t *testing.T) {
	index := NewTestIndex()

	item := item{"val", 0, time.Now()}
	for i := 0; i < 1024; i++ {
		index.Put(intKey(i), item.Val(), item.Ver(), item.Time)
	}

	i := 0
	index.Scan(func(s *Scan, k Key, v Item) {
		assert.Equal(t, i, int(k.(intKey)))
		assert.Equal(t, item, v)
		i++
	})

	assert.Equal(t, 1024, i)
}

func TestIndex_Scan_Stop(t *testing.T) {
	index := NewTestIndex()

	item := item{"val", 0, time.Now()}
	for i := 0; i < 1024; i++ {
		index.Put(intKey(i), item.Val(), item.Ver(), item.Time)
	}

	i := 0
	index.Scan(func(s *Scan, k Key, v Item) {
		iter := k.(intKey)
		if iter == 512 {
			s.Stop()
			return
		}

		i++
	})

	assert.Equal(t, 512, i)
}

func TestIndex_Scan_Skip_GreaterThanMax(t *testing.T) {
	index := NewTestIndex()

	item := item{"val", 0, time.Now()}
	for i := 0; i < 1024; i++ {
		index.Put(intKey(i), item.Val(), item.Ver(), item.Time)
	}

	i := 0
	index.Scan(func(s *Scan, k Key, v Item) {
		s.Next(intKey(1025))
		i++
	})

	assert.Equal(t, 1, i)
}

func TestIndex_Scan_Skip(t *testing.T) {
	index := NewTestIndex()

	item := item{"val", 0, time.Now()}
	for i := 0; i < 1024; i++ {
		index.Put(intKey(i), item.Val(), item.Ver(), item.Time)
	}

	i := 0
	index.Scan(func(s *Scan, k Key, v Item) {
		iter := k.(intKey)
		if iter%2 == 0 {
			s.Next(iter + 1) // lots of skips
			return
		}

		i++
	})

	assert.Equal(t, 512, i)
}
