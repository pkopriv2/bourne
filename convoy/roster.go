package convoy

// type update interface {
// MemberId() uuid.UUID
// Version() int
// Key() string
// Val() string
// }
//
// type roster struct {
// lock    sync.RWMutex
// updates map[uuid.UUID]update
// }
//
// func newRoster() Roster {
// return &roster{updates: make(map[uuid.UUID]update)}
// }
//
// func (r *roster) Iterator() Iterator {
// return newMemberIterator(r)
// }
//
// func (r *roster) Size() int {
// count := 0
//
// iter := r.Iterator()
// for iter.Next() != nil {
// count++
// }
//
// return count
// }
//
// func (r *roster) Get(id uuid.UUID) Member {
// return updateToMember(r.get(id))
// }
//
// func (r *roster) get(id uuid.UUID) update {
// r.lock.RLock()
// defer r.lock.RUnlock()
// return r.updates[id]
// }
//
// func (r *roster) log() []update {
// r.lock.RLock()
// defer r.lock.RUnlock()
// return indexedUpdatesToUpdates(r.updates)
// }
//
// func (r *roster) join(m Member) bool {
// r.lock.Lock()
// defer r.lock.Unlock()
// return applyUpdate(r.updates, newJoin(m))
// }
//
// func (r *roster) leave(id uuid.UUID, version int) bool {
// r.lock.Lock()
// defer r.lock.Unlock()
// return applyUpdate(r.updates, newLeave(id, version))
// }
//
// // func (r *roster) fail(m Member) bool {
// // r.lock.Lock()
// // defer r.lock.Unlock()
// // return applyUpdate(r.updates, newFail(m))
// // }
//
// // A basic randomized iterator.
// type iterator struct {
// roster *roster
// order  []uuid.UUID
// idx    int
// }
//
// func newIterator(roster *roster) *iterator {
// return &iterator{
// roster: roster,
// order:  shuffleIds(updatesToIds(roster.log()))}
// }
//
// func (i *iterator) Next() update {
// defer func() {i.idx++}()
//
// if i.idx == len(i.order) {
// return nil
// }
//
// return i.roster.get(i.order[i.idx])
// }
//
// type memberIterator struct {
// inner *iterator
// }
//
// func newMemberIterator(roster *roster) *memberIterator {
// return &memberIterator{newIterator(roster)}
// }
//
// func (m *memberIterator) Next() Member {
// for {
// u := m.inner.Next()
// if u == nil {
// return nil
// }
//
// if m := updateToMember(u); m != nil {
// return m
// }
// }
// }
//
//
// func updateToMember(u update) Member {
// switch t := u.(type) {
// case *leave:
// return nil
// // case *fail:
// // return t.member
// case *join:
// return t.member
// }
//
// panic("Unknown update type!")
// }
//
// func indexedUpdatesToUpdates(index map[uuid.UUID]update) []update {
// values := make([]update, 0, len(index))
// for _, v := range index {
// values = append(values, v)
// }
//
// return values
// }
//
// func updatesToIds(updates []update) []uuid.UUID {
// ids := make([]uuid.UUID, 0, len(updates))
// for _, u := range updates {
// ids = append(ids, u.Re())
// }
//
// return ids
// }
//
// func shuffleIds(ids []uuid.UUID) []uuid.UUID {
// perm := rand.Perm(len(ids))
//
// ret := make([]uuid.UUID, len(ids))
// for i, idx := range perm {
// ret[i] = ids[idx]
// }
//
// return ret
// }
//
// func applyUpdate(init map[uuid.UUID]update, u update) bool {
// memberId := u.Re()
//
// cur := init[memberId]
// if cur == nil {
// init[memberId] = u
// return true
// }
//
// if cur.Version() < u.Version() {
// init[memberId] = u
// return true
// }
//
// if _, ok := u.(*leave); ok {
// init[memberId] = u
// return true
// }
//
// return false
// }
