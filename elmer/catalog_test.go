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
	store := catalogue.Init([]byte("store"))
	assert.NotNil(t, store)
}

func TestCatalogue_New_Exist(t *testing.T) {
	catalogue := newCatalog()
	store1 := catalogue.Init([]byte("store"))
	assert.NotNil(t, store1)

	store2 := catalogue.Init([]byte("store"))
	assert.Equal(t, store1, store2)
}

func TestCatalogue_Del_NoExist(t *testing.T) {
	catalogue := newCatalog()
	catalogue.Del([]byte("store"))
}
