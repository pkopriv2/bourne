package elmer

// import (
	// "github.com/boltdb/bolt"
	// "github.com/pkg/errors"
	// "github.com/pkopriv2/bourne/common"
	// "github.com/pkopriv2/bourne/scribe"
	// "github.com/pkopriv2/bourne/stash"
// )
//
// // Bolt implementation of kayak log store.
// var (
	// dataBucket = []byte("elmer.data")
	// metaBucket = []byte("elmer.meta")
	// infoKey    = stash.String("elmer.Info")
// )
//
// func initBoltBuckets(db *bolt.DB) (err error) {
	// return db.Update(func(tx *bolt.Tx) (err error) {
		// var e error
		// _, e = tx.CreateBucketIfNotExists(dataBucket)
		// err = common.Or(err, e)
		// _, e = tx.CreateBucketIfNotExists(metaBucket)
		// err = common.Or(err, e)
		// return
	// })
// }
//
// type boltInfoDat struct {
	// Ver     int
	// Enabled bool
// }
//
// func (b boltInfoDat) Write(w scribe.Writer) {
	// w.WriteInt("ver", b.Ver)
	// w.WriteBool("enabled", b.Enabled)
// }
//
// // TODO: Replace with boltdb implementation
// type boltStore struct {
	// // the path represents the versioned location in the meta hierarchy
	// path path
//
	// // the meta index hosts the store data (which builds a hierarchy)
	// db *bolt.DB
// }
//
// func newBoltStore(path []segment, db *bolt.DB) *boltStore {
	// return &boltStore{path, db}
// }
//
// func (s *boltStore) Close() error {
	// return nil
// }
//
// func (s *boltStore) Info() storeInfo {
	// return storeInfo{s.Path(), true}
// }
//
// func (s *boltStore) Path() path {
	// return s.path
// }
//
// func (s *boltStore) Base() segment {
	// return s.path[len(s.path)-1]
// }
//
// func (s *boltStore) Version() int {
	// return s.Base().Ver
// }
//
// func (s *boltStore) Name() []byte {
	// return s.Base().Elem
// }
//
// func (s *boltStore) loadStoreBucket(tx *bolt.Tx, path path) (store *bolt.Bucket, err error) {
	// cur := tx.Bucket(dataBucket) // guaranteed to exist
	// for _, elem := range path {
		// cur = cur.Bucket(stash.Key(elem.Elem).ChildInt(elem.Ver))
		// if cur == nil {
			// return nil, errors.Wrapf(PathError, "Path %v contains missing store: %v", path, elem)
		// }
	// }
	// return cur, nil
// }
//
// func (s *boltStore) readInfo(bucket *bolt.Bucket) (boltInfoDat, error) {
	// infoBytes := bucket.Get(infoKey)
	// for _, elem := range path {
		// cur = cur.Bucket(stash.Key())
		// if cur == nil {
			// return nil, errors.Wrapf(PathError, "Path %v contains missing store: %v", path, elem)
		// }
	// }
	// return cur, nil
// }
//
// func (s *boltStore) loadInfo(bucket *bolt.Bucket, key []byte) (info storeInfo, err error) {
	// // bytes := bucket.Get(key)
	// return
// }
//
// func (s *boltStore) RecurseInfo(parent path, child []byte) (info storeInfo, found bool, err error) {
	// err = s.db.View(func(tx *bolt.Tx) error {
		// bucket, err := s.loadStoreBucket(tx, parent)
		// if err != nil {
			// return errors.WithStack(err)
		// }
//
		// return nil
		// // bucket.Get(child)
	// })
	// return storeInfo{}, false, nil
// }
//
// func (s *boltStore) RecurseEnable(prev path) (*boltStore, bool, error) {
	// return nil, false, nil
// }
//
// func (s *boltStore) RecurseDisable(path path) (storeInfo, bool, error) {
	// return storeInfo{}, false, nil
// }
//
// func (s *boltStore) RecurseItemRead(path path, key []byte) (Item, bool, error) {
	// return Item{}, false, nil
// }
//
// func (s *boltStore) RecurseItemSwap(path path, key []byte, val []byte, del bool, seq int, prev int) (Item, bool, error) {
	// return Item{}, false, nil
// }
//
// func (s *boltStore) Read(key []byte) (item Item, ok bool) {
	// return
// }
//
// func (s *boltStore) Swap(key []byte, val []byte, del bool, seq int, prev int) (item Item, ok bool) {
	// return
// }
