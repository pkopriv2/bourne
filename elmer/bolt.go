package elmer

// import (
	// "github.com/boltdb/bolt"
	// "github.com/pkg/errors"
	// "github.com/pkopriv2/bourne/amoeba"
// )
//
// // Bolt implementation of kayak log store.
// var (
	// dataBucket            = []byte("elmer.data")
	// metaBucket            = []byte("elmer.meta")
// )
//
// func initBoltBuckets(db *bolt.DB) (err error) {
	// return db.Update(func(tx *bolt.Tx) error {
		// var e error
		// _, e = tx.CreateBucketIfNotExists(logBucket)
		// err = common.Or(err, e)
		// _, e = tx.CreateBucketIfNotExists(logItemBucket)
		// return common.Or(err, e)
	// })
// }
//
//
// // TODO: Replace with boltdb implementation
// type boltStore struct {
	// // the path represents the versioned location in the meta hierarchy
	// path path
//
	// // the meta index hosts the store data (which builds a hierarchy)
	// db bolt.DB
// }
//
// func newBoltStore(path []segment, db *bolt.DB) *store {
	// return &store{path, db}
// }
//
// func (s *store) Close() error {
	// return nil
// }
//
// func (s *store) Dat() storeDat {
	// return storeDat{s.Version(), s}
// }
//
// func (s *store) Info() storeInfo {
	// return storeInfo{s.Path(), true}
// }
//
// func (s *store) Path() path {
	// return s.path
// }
//
// func (s *store) Base() segment {
	// return s.path[len(s.path)-1]
// }
//
// func (s *store) Version() int {
	// return s.Base().Ver
// }
//
// func (s *store) Name() []byte {
	// return s.Base().Elem
// }
//
// func (s *store) Load(path path) (*store, error) {
	// cur := s
	// for _, elem := range path {
		// cur = cur.ChildLoad(elem.Elem, elem.Ver)
		// if cur == nil {
			// return nil, errors.Wrapf(PathError, "Path %v contains missing store: %v", path, elem)
		// }
	// }
	// return cur, nil
// }
//
// func (s *store) RecurseInfo(parent path, child []byte) (storeInfo, bool, error) {
	// dest, err := s.Load(parent)
	// if err != nil {
		// return storeInfo{}, false, err
	// }
//
	// info, ok := dest.ChildInfo(child)
	// return info, ok, nil
// }
//
// func (s *store) RecurseEnable(prev path) (*store, bool, error) {
	// dest, err := s.Load(prev.Parent())
	// if err != nil {
		// return nil, false, err
	// }
//
	// store, ok := dest.ChildEnable(prev.Tail())
	// return store, ok, nil
// }
//
// func (s *store) RecurseDisable(path path) (storeInfo, bool, error) {
	// dest, err := s.Load(path.Parent())
	// if err != nil {
		// return storeInfo{}, false, err
	// }
//
	// leaf := path.Last()
	// info, ok := dest.ChildDisable(leaf.Elem, leaf.Ver)
	// return info, ok, nil
// }
//
// func (s *store) RecurseItemRead(path path, key []byte) (Item, bool, error) {
	// dest, err := s.Load(path)
	// if err != nil {
		// return Item{}, false, errors.Wrapf(err, "Error while reading key [%v]", key)
	// }
//
	// item, ok := dest.Read(key)
	// return item, ok, nil
// }
//
// func (s *store) RecurseItemSwap(path path, key []byte, val []byte, del bool, seq int, prev int) (Item, bool, error) {
	// dest, err := s.Load(path)
	// if err != nil {
		// return Item{}, false, errors.Wrapf(err, "Error while swapping key [%v]", key)
	// }
//
	// item, ok := dest.Swap(key, val, del, seq, prev)
	// return item, ok, nil
// }
//
// func (s *store) ChildPath(name []byte, ver int) path {
	// return path(s.path).Child(name, ver)
// }
//
// func (s *store) ChildInit(name []byte, ver int) *store {
	// return newStore(path(s.path).Child(name, ver))
// }
//
// func (s *store) ChildInfo(name []byte) (info storeInfo, ok bool) {
	// s.meta.Read(func(u amoeba.View) {
		// raw := u.Get(amoeba.BytesKey(name))
		// if raw == nil {
			// return
		// }
//
		// item := raw.(storeDat)
		// if item.Raw == nil {
			// info, ok = storeInfo{s.Path().Child(name, item.Ver), false}, true
		// } else {
			// info, ok = storeInfo{item.Raw.Path(), true}, true
		// }
	// })
	// return
// }
//
// func (s *store) ChildLoad(name []byte, ver int) (ret *store) {
	// s.meta.Read(func(u amoeba.View) {
		// raw := u.Get(amoeba.BytesKey(name))
		// if raw == nil {
			// return
		// }
//
		// item := raw.(storeDat)
		// if item.Ver != ver {
			// return
		// }
//
		// ret = item.Raw
	// })
	// return
// }
//
// func (s *store) ChildEnable(name []byte, prev int) (ret *store, ok bool) {
	// s.meta.Update(func(u amoeba.Update) {
		// raw := u.Get(amoeba.BytesKey(name))
		// if raw == nil {
			// if prev != -1 {
				// return
			// }
//
			// ret, ok = s.ChildInit(name, prev+1), true
			// u.Put(amoeba.BytesKey(name), ret.Dat())
			// return
		// }
//
		// item := raw.(storeDat)
		// if item.Ver != prev || item.Raw != nil {
			// return
		// }
//
		// ret, ok = s.ChildInit(name, prev+1), true
		// u.Put(amoeba.BytesKey(name), ret.Dat())
	// })
	// return
// }
//
// func (s *store) ChildDisable(name []byte, prev int) (info storeInfo, ok bool) {
	// s.meta.Update(func(u amoeba.Update) {
		// raw := u.Get(amoeba.BytesKey(name))
		// if raw == nil {
			// return
		// }
//
		// item := raw.(storeDat)
		// if item.Ver != prev || item.Raw == nil {
			// return
		// }
//
		// u.Put(amoeba.BytesKey(name), storeDat{prev + 1, nil})
		// info, ok = storeInfo{s.Path().Child(name, prev+1), false}, true
	// })
	// return
// }
//
// func (s *store) Read(key []byte) (item Item, ok bool) {
	// s.data.Read(func(u amoeba.View) {
		// raw := u.Get(amoeba.BytesKey(key))
		// if raw == nil {
			// return
		// }
//
		// item, ok = raw.(Item), true
	// })
	// return
// }
//
// func (s *store) Swap(key []byte, val []byte, del bool, seq int, prev int) (item Item, ok bool) {
	// item = Item{key, val, prev + 1, false, seq}
	// s.data.Update(func(u amoeba.Update) {
		// raw := u.Get(amoeba.BytesKey(key))
		// if raw == nil {
			// if prev != -1 {
				// return
			// }
//
			// ok = true
			// u.Put(amoeba.BytesKey(key), item)
			// return
		// }
//
		// if cur := raw.(Item); cur.Ver == prev {
			// u.Put(amoeba.BytesKey(key), item)
			// ok = true
			// return
		// }
	// })
	// return
// }
//
// // func storeAll(idx amoeba.Index) (items []Item, ok bool) {
// // ret := make([]Item, 0, idx.Size())
// // idx.Read(func(u amoeba.View) {
// // u.Scan(func(s amoeba.Scan, k amoeba.Key, i interface{}) {
// // ret = append(ret, i.(Item))
// // })
// // })
// // return
// // }
