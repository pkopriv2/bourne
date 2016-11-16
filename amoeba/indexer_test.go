package amoeba

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func NewTestIndexer() Indexer {
	return NewIndexer(
		common.NewContext(
			common.NewConfig(map[string]interface{}{
				"amoeba.index.gc.expiration": 100 * time.Millisecond,
				"amoeba.index.gc.cycle":      10 * time.Millisecond,
			})))
}

func TestIndexer_Close(t *testing.T) {
	idx := NewTestIndexer()
	assert.Nil(t, idx.Close())
}

func TestIndexer_Close_AlreadyClosed(t *testing.T) {
	idx := NewTestIndexer()
	assert.Nil(t, idx.Close())
	assert.NotNil(t, idx.Close())
}

// should test mutual exclusivity of close method

func TestIndexerGc_Empty(t *testing.T) {
	idx := NewTestIndexer()
	time.Sleep(200 * time.Millisecond)
	assert.Nil(t, idx.Close())
}

func TestIndexerGc_Single(t *testing.T) {
	idx := NewTestIndexer()

	Del(idx, intKey(1), 1)
	time.Sleep(200 * time.Millisecond)
	assert.Nil(t, idx.Close())
}
