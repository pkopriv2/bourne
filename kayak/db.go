package kayak

import (
	"sync"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
)

type database struct {
	ctx    common.Context
	data   amoeba.Index
	lock   sync.RWMutex
	closed bool
}


// Helper functions.

func dbGetItem(data amoeba.View, key string) *Item {
	if raw := data.Get(amoeba.StringKey(key)); raw != nil {
		item := raw.(Item)
		return &item
	}
	return nil
}

func dbDelItem(data amoeba.Update, key string, ver int) (bool, error) {
	exp := dbGetItem(data, key)
	if exp.Ver != ver {
		return false, nil
	}

	return false, nil
	// new := Item{"", chg.Ver, true}
	// data.Put(amoeba.StringKey(key), new)
	// return true, new, nil
}

func dbPutItem(data amoeba.Update, key string, val string, expVer int) (bool, *Item, error) {
	cur := dbGetItem(data, key)
	if expVer != cur.Ver {
		return false, nil, nil
	}

	// new := Item{val, chg.Ver, false}
	// data.Put(amoeba.StringKey(key), new)
	return false, nil, nil
}
