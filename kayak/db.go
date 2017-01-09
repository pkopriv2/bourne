package kayak

// // the core store type
// type store struct {
// data      amoeba.Index
// listeners concurrent.List
// closed    chan struct{}
// closer    chan struct{}
// }
//
// func newStore() *store {
// return &store{
// data:      amoeba.NewBTreeIndex(32),
// listeners: concurrent.NewList(8),
// closed:    make(chan struct{}),
// closer:    make(chan struct{}, 1)}
// }
//
// func (s *store) Close() error {
// select {
// case <-s.closed:
// return ClosedError
// case s.closer <- struct{}{}:
// }
//
// for _, l := range s.Listeners() {
// close(l)
// }
//
// close(s.closed)
// return nil
// }
//
// func (s *store) Listeners() (ret []chan Item) {
// all := s.listeners.All()
// ret = make([]chan Item, 0, len(all))
// for _, l := range all {
// ret = append(ret, l.(chan Item))
// }
// return
// }
//
// func (s *store) Broadcast(i Item) error {
// for _, l := range s.Listeners() {
// // do not close while broadcasting
// select {
// case <-s.closed:
// return ClosedError
// case s.closer <- struct{}{}:
// }
// l <- i // this WILL block closing! aaaaaaa!
// <-s.closer
// }
// return nil
// }
//
// func (s *store) Get(key []byte) (item Item, err error) {
// s.data.Read(func(v amoeba.View) {
// raw := v.Get(amoeba.BytesKey(key))
// if raw == nil {
// return
// }
//
// item = raw.(Item)
// })
// return
// }
//
// func (s *store) Put(key []byte, val []byte, prev int) (item Item, err error) {
// s.data.Update(func(u amoeba.Update) {
// raw := u.Get(amoeba.BytesKey(key))
// if raw == nil {
// if prev != 0 {
// return
// }
// } else {
// if prev != raw.(Item).Ver {
// return
// }
// }
//
// item = Item{key, val, prev + 1, u.Time()}
// u.Put(amoeba.BytesKey(key), item)
// s.Broadcast(item)
// })
// return
// }
//
// func (s *store) Del(key []byte, prev int) (item Item, err error) {
// s.data.Update(func(u amoeba.Update) {
// raw := u.Get(amoeba.BytesKey(key))
// if raw == nil {
// if prev != 0 {
// return
// }
// } else {
// if prev != raw.(Item).Ver {
// return
// }
// }
//
// item = Item{key, nil, prev + 1, u.Time()}
// u.Put(amoeba.BytesKey(key), item)
// s.Broadcast(item)
// })
// return
// }
