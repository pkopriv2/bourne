package elmer

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

type storeInfo struct {
	Ver   int
	Store *store
}

type store struct {
	path    []segment
	catalog amoeba.Index
	data    amoeba.Index
}

func newStore(path []segment) *store {
	return &store{path, amoeba.NewBTreeIndex(32), amoeba.NewBTreeIndex(32)}
}

func (s *store) Close() error {
	return nil
}

func (s *store) Item() storeInfo {
	return storeInfo{s.Version(), s}
}

func (s *store) Path() []segment {
	return s.path
}

func (s *store) Base() segment {
	return s.path[len(s.path)-1]
}

func (s *store) Version() int {
	return s.Base().Ver
}

func (s *store) Name() []byte {
	return s.Base().Elem
}

func (s *store) Load(path path) (*store, error) {
	cur := s
	for _, elem := range path {
		cur = cur.ChildGet(elem.Elem, elem.Ver)
		if cur == nil {
			return nil, errors.Wrapf(PathError, "Path %v contains missing store: %v", path, elem)
		}
	}
	return cur, nil
}

func (s *store) RecurseInfo(parent path, child []byte) (storeInfo, bool, error) {
	dest, err := s.Load(parent)
	if err != nil {
		return storeInfo{}, false, err
	}

	info, ok := dest.ChildInfo(child)
	return info, ok, nil
}

func (s *store) RecurseEnable(path path) (*store, bool, error) {
	dest, err := s.Load(path.Parent())
	if err != nil {
		return nil, false, err
	}

	leaf := path.Leaf()
	store, ok := dest.ChildEnableOrCreate(leaf.Elem, leaf.Ver-1)
	return store, ok, nil
}

func (s *store) RecurseDisable(path path) (bool, error) {
	dest, err := s.Load(path.Parent())
	if err != nil {
		return false, err
	}

	leaf := path.Leaf()
	return dest.ChildDisable(leaf.Elem, leaf.Ver), nil
}

func (s *store) RecurseItemRead(path path, key []byte) (Item, bool, error) {
	dest, err := s.Load(path)
	if err != nil {
		return Item{}, false, errors.Wrapf(err, "Error while reading key [%v]", key)
	}

	item, ok := dest.Get(key)
	return item, ok, nil
}

func (s *store) RecurseItemSwap(path path, key []byte, val []byte, del bool, ver int) (Item, bool, error) {
	dest, err := s.Load(path)
	if err != nil {
		return Item{}, false, errors.Wrapf(err, "Error while swapping key [%v]", key)
	}

	item, ok := dest.Swap(key, val, del, ver)
	return item, ok, nil
}

func (s *store) ChildPath(name []byte, ver int) []segment {
	return path(s.path).Child(name, ver)
}

func (s *store) ChildInit(name []byte, ver int) *store {
	return newStore(path(s.path).Child(name, ver))
}

func (s *store) ChildInfo(name []byte) (info storeInfo, ok bool) {
	s.catalog.Read(func(u amoeba.View) {
		raw := u.Get(amoeba.BytesKey(name))
		if raw == nil {
			return
		}

		info, ok = raw.(storeInfo), true
	})
	return
}

func (s *store) ChildGet(name []byte, ver int) (ret *store) {
	s.catalog.Read(func(u amoeba.View) {
		raw := u.Get(amoeba.BytesKey(name))
		if raw == nil {
			return
		}

		ret = raw.(storeInfo).Store
	})
	return
}

func (s *store) ChildEnableOrCreate(name []byte, prev int) (ret *store, ok bool) {
	s.catalog.Update(func(u amoeba.Update) {
		raw := u.Get(amoeba.BytesKey(name))
		if raw == nil {
			if prev != -1 {
				return
			}

			ret, ok = s.ChildInit(name, prev+1), true
			u.Put(amoeba.BytesKey(name), ret.Item())
			return
		}

		item := raw.(storeInfo)
		if item.Ver != prev {
			return
		}

		ret, ok = s.ChildInit(name, prev+1), true
		u.Put(amoeba.BytesKey(name), ret.Item())
	})
	return
}

func (s *store) ChildDisable(name []byte, prev int) (ok bool) {
	s.catalog.Update(func(u amoeba.Update) {
		raw := u.Get(amoeba.BytesKey(name))
		if raw == nil {
			return
		}

		item := raw.(storeInfo)
		if item.Ver != prev {
			return
		}

		u.Put(amoeba.BytesKey(name), storeInfo{prev + 1, nil})
		ok = true
	})
	return
}

func (s *store) Get(key []byte) (item Item, ok bool) {
	s.data.Read(func(u amoeba.View) {
		raw := u.Get(amoeba.BytesKey(key))
		if raw == nil {
			return
		}

		item, ok = raw.(Item), true
	})
	return
}

func (s *store) Swap(key []byte, val []byte, del bool, prev int) (item Item, ok bool) {
	item = Item{key, val, prev + 1, false}
	s.data.Update(func(u amoeba.Update) {
		raw := u.Get(amoeba.BytesKey(key))
		if raw == nil {
			if ok = prev == 0; ok {
				u.Put(amoeba.BytesKey(key), item)
			}
			return
		}

		if cur := raw.(Item); cur.Ver == prev {
			u.Put(amoeba.BytesKey(key), item)
			ok = true
			return
		}
	})
	return
}

func storeAll(idx amoeba.Index) (items []Item, ok bool) {
	ret := make([]Item, 0, idx.Size())
	idx.Read(func(u amoeba.View) {
		u.Scan(func(s amoeba.Scan, k amoeba.Key, i interface{}) {
			ret = append(ret, i.(Item))
		})
	})
	return
}

var emptyPath = path([]segment{})

type segment struct {
	Elem []byte
	Ver  int
}

func (s segment) Write(w scribe.Writer) {
	w.WriteBytes("elem", s.Elem)
	w.WriteInt("ver", s.Ver)
}

func readSegment(r scribe.Reader) (s segment, e error) {
	e = r.ReadBytes("elem", &s.Elem)
	e = common.Or(e, r.ReadInt("ver", &s.Ver))
	return
}

func segmentParser(r scribe.Reader) (interface{}, error) {
	return readSegment(r)
}

type path []segment

func (p path) String() string {
	if len(p) == 0 {
		return ""
	}

	return fmt.Sprintf("%v:%v/%v", p[0].Elem, p[0].Ver, path(p[1:]))
}

func (p path) Parent() path {
	return p[:len(p)-1]
}

func (p path) Child(elem []byte, ver int) path {
	return append(p, segment{elem, ver})
}

func (p path) Leaf() segment {
	return p[len(p)-1]
}

func (p path) Write(w scribe.Writer) {
	w.WriteMessages("raw", []segment(p))
}

func readPath(r scribe.Reader) (p path, e error) {
	e = r.ParseMessages("raw", (*[]segment)(&p), segmentParser)
	return
}

func pathParser(r scribe.Reader) (interface{}, error) {
	return readPath(r)
}
