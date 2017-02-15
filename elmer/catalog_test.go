package elmer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCatalogue_Get_NoExist(t *testing.T) {
	catalogue := newCatalog()
	store := catalogue.Get([]byte{})
	assert.Nil(t, store)
}

func TestCatalogue_New_NoExist(t *testing.T) {
	catalogue := newCatalog()
	store := catalogue.Ensure([]byte("store"))
	assert.NotNil(t, store)
}

func TestCatalogue_New_Exist(t *testing.T) {
	catalogue := newCatalog()
	store1 := catalogue.Ensure([]byte("store"))
	assert.NotNil(t, store1)

	store2 := catalogue.Ensure([]byte("store"))
	assert.Equal(t, store1, store2)
}

func TestCatalogue_Del_NoExist(t *testing.T) {
	catalogue := newCatalog()
	catalogue.Del([]byte("store"))
}

func TestCatalogue_Del_Exist(t *testing.T) {
	catalogue := newCatalog()

	store1 := catalogue.Ensure([]byte("store"))
	assert.NotNil(t, store1)

	catalogue.Del([]byte("store"))
	store2 := catalogue.Get([]byte("store"))
	assert.Nil(t, store2)
}

func TestStore_Get_NoExist(t *testing.T) {
	catalogue := newCatalog()

	store1 := catalogue.Ensure([]byte("store"))
	assert.NotNil(t, store1)

	_, ok := store1.Get([]byte("key"))
	assert.False(t, ok)
}

func TestStore_Swap_NoExist(t *testing.T) {
	catalogue := newCatalog()

	store1 := catalogue.Ensure([]byte("store"))
	assert.NotNil(t, store1)

	_, ok := store1.Swap([]byte("key"), []byte("val"), 1)
	assert.False(t, ok)
}

func TestStore_Swap_First(t *testing.T) {
	catalogue := newCatalog()

	store1 := catalogue.Ensure([]byte("store"))
	assert.NotNil(t, store1)

	item, ok := store1.Swap([]byte("key"), []byte("val"), 0)
	assert.True(t, ok)
	assert.Equal(t, 1, item.Ver)
}

func TestStore_Swap_ExistMatch(t *testing.T) {
	catalogue := newCatalog()

	store1 := catalogue.Ensure([]byte("store"))
	assert.NotNil(t, store1)

	item, ok := store1.Swap([]byte("key"), []byte("val"), 0)
	assert.True(t, ok)

	item2, ok := store1.Swap([]byte("key"), []byte("val2"), item.Ver)
	assert.True(t, ok)
	assert.Equal(t, 2, item2.Ver)
}

func TestStore_Swap_ExistNoMatch(t *testing.T) {
	catalogue := newCatalog()

	store1 := catalogue.Ensure([]byte("store"))
	assert.NotNil(t, store1)

	_, ok := store1.Swap([]byte("key"), []byte("val"), 0)
	assert.True(t, ok)

	_, ok = store1.Swap([]byte("key"), []byte("val2"), 2)
	assert.False(t, ok)
}

func TestStore_Get_Exist(t *testing.T) {
	catalogue := newCatalog()

	store1 := catalogue.Ensure([]byte("store"))
	assert.NotNil(t, store1)

	_, ok := store1.Swap([]byte("key"), []byte("val"), 0)
	assert.True(t, ok)

	_, ok = store1.Get([]byte("key"))
	assert.True(t, ok)
}
