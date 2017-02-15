package elmer

import (
	"sync"

	"github.com/pkopriv2/bourne/amoeba"
)

type catalog struct {
	stores     map[string]*store
	storesLock sync.RWMutex
}

func newCatalog() *catalog {
	return &catalog{stores: make(map[string]*store)}
}

func (c *catalog) Del(name []byte) {
	c.storesLock.Lock()
	defer c.storesLock.Unlock()
	delete(c.stores, string(name))
}

func (c *catalog) Get(name []byte) *store {
	c.storesLock.RLock()
	defer c.storesLock.RUnlock()
	if store, ok := c.stores[string(name)]; ok {
		return store
	} else {
		return nil
	}
}

func (c *catalog) Ensure(name []byte) *store {
	if store := c.Get(name); store != nil {
		return store
	}

	c.storesLock.Lock()
	defer c.storesLock.Unlock()
	if store, ok := c.stores[string(name)]; ok {
		return store
	}

	store := newStore(name)
	c.stores[string(name)] = store
	return store
}

type store struct {
	name []byte
	raw  amoeba.Index
}

func newStore(name []byte) *store {
	return &store{name, amoeba.NewBTreeIndex(32)}
}

func (s *store) Close() error {
	return nil
}

func (s *store) Get(key []byte) (Item, bool) {
	return read(s.raw, key)
}

func (s *store) Swap(key []byte, val []byte, ver int) (Item, bool) {
	return swap(s.raw, s.name, key, val, ver)
}

func (s *store) Put(key []byte, val []byte, ver int) (Item, bool) {
	return swap(s.raw, s.name, key, val, ver)
}

func (s *store) Del(key []byte, ver int) bool {
	_, ok := swap(s.raw, s.name, key, nil, ver)
	return ok
}

func swap(idx amoeba.Index, store []byte, key []byte, val []byte, prev int) (item Item, ok bool) {
	bytesKey := amoeba.BytesKey(item.Key)

	item = Item{store, key, val, prev + 1}
	idx.Update(func(u amoeba.Update) {
		raw := u.Get(bytesKey)
		if raw == nil {
			if ok = prev == 0; ok {
				u.Put(bytesKey, item)
			}
			return
		}

		if cur := raw.(Item); cur.Ver == prev {
			u.Put(bytesKey, item)
			ok = true
			return
		}
	})
	return
}

func read(idx amoeba.Index, key []byte) (item Item, ok bool) {
	idx.Update(func(u amoeba.Update) {
		raw := u.Get(amoeba.BytesKey(key))
		if raw == nil {
			return
		}

		item, ok = raw.(Item), true
	})
	return
}
