package elmer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPath_Empty_String(t *testing.T) {
	assert.Equal(t, "", emptyPath.String())
}

// func TestPath_Single_String(t *testing.T) {
// path := emptyPath.Sub([]byte{0}, 0)
// assert.Equal(t, "", path.String())
// }
//
// func TestPath_Multi_String(t *testing.T) {
// path := emptyPath.Sub([]byte{0}, 0).Sub([]byte{1}, 1)
// assert.Equal(t, "", path.String())
// }

func TestStore_ChildEnableOrCreate_NoPrevious(t *testing.T) {
	store := newStore([]segment{})
	sub, ok := store.ChildEnableOrCreate([]byte("store"), -1)
	assert.True(t, ok)
	assert.NotNil(t, sub)
}

func TestStore_ChildEnableOrCreate_CompareFail(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildEnableOrCreate([]byte("store"), -1)
	assert.True(t, ok)

	_, ok = root.ChildEnableOrCreate([]byte("store"), 1)
	assert.False(t, ok)
}

func TestStore_ChildEnableOrCreate(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildEnableOrCreate([]byte("store"), -1)
	assert.True(t, ok)

	_, ok = root.ChildEnableOrCreate([]byte("store"), 0)
	assert.True(t, ok)
}

func TestStore_ChildInfo_NoExist(t *testing.T) {
	root := newStore([]segment{})
	info, found := root.ChildInfo([]byte("store"))
	assert.False(t, found)
	assert.Nil(t, info.Store)
}

func TestStore_ChildInfo(t *testing.T) {
	root := newStore([]segment{})
	_, ok := root.ChildEnableOrCreate([]byte("store"), -1)
	assert.True(t, ok)

	info, found := root.ChildInfo([]byte("store"))
	assert.True(t, found)
	assert.NotNil(t, info.Store)
}

func TestStore_ChildGet_Exist(t *testing.T) {
	store := newStore([]segment{})
	_, ok := store.ChildEnableOrCreate([]byte("store"), -1)
	assert.True(t, ok)

	sub := store.ChildGet([]byte("store"), 0)
	assert.NotNil(t, sub)
}

func TestStore_ChildDisable_NoExist(t *testing.T) {
	store := newStore([]segment{})
	assert.False(t, store.ChildDisable([]byte("store"), -1))
}

func TestStore_ChildDisable(t *testing.T) {
	store := newStore([]segment{})

	_, ok := store.ChildEnableOrCreate([]byte("store"), -1)
	assert.True(t, ok)
	assert.True(t, store.ChildDisable([]byte("store"), 0))
}

// func TestStore_Get_NoExist(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// _, ok := store1.Get([]byte("key"))
// assert.False(t, ok)
// }
//
// func TestStore_Swap_NoExist(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// _, ok := store1.Swap([]byte("key"), []byte("val"), 1)
// assert.False(t, ok)
// }
//
// func TestStore_Swap_First(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// item, ok := store1.Swap([]byte("key"), []byte("val"), 0)
// assert.True(t, ok)
// assert.Equal(t, 1, item.Ver)
// }
//
// func TestStore_Swap_ExistMatch(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// item, ok := store1.Swap([]byte("key"), []byte("val"), 0)
// assert.True(t, ok)
//
// item2, ok := store1.Swap([]byte("key"), []byte("val2"), item.Ver)
// assert.True(t, ok)
// assert.Equal(t, 2, item2.Ver)
// }
//
// func TestStore_Swap_ExistNoMatch(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// _, ok := store1.Swap([]byte("key"), []byte("val"), 0)
// assert.True(t, ok)
//
// _, ok = store1.Swap([]byte("key"), []byte("val2"), 2)
// assert.False(t, ok)
// }
//
// func TestStore_Get_Exist(t *testing.T) {
// catalog := newCatalog()
//
// store1 := catalog.Ensure([]byte("store"))
// assert.NotNil(t, store1)
//
// _, ok := store1.Swap([]byte("key"), []byte("val"), 0)
// assert.True(t, ok)
//
// _, ok = store1.Get([]byte("key"))
// assert.True(t, ok)
// }
