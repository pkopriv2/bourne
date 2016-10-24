package convoy

import (
	"math/rand"
	"sync"

	uuid "github.com/satori/go.uuid"
)

func newJoin(member Member) update {
	return &join{member}
}

func newLeave(memberId uuid.UUID, version int) update {
	return &leave{memberId, version}
}

func newFail(memberId uuid.UUID, version int) update {
	return &fail{memberId, version}
}

type roster struct {
	lock    sync.RWMutex
	updates map[uuid.UUID]update
}

func newRoster() Roster {
	return &roster{updates: make(map[uuid.UUID]update)}
}

func (r *roster) Iterator() Iterator {
	return NewIterator(r)
}

func (r *roster) Size() int {
	count := 0

	iter := r.Iterator()
	for iter.Next() != nil {
		count++
	}

	return count
}

func (r *roster) Get(id uuid.UUID) Member {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return updateToMember(r.updates[id])
}

func (r *roster) log() []update {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return indexedUpdatesToUpdates(r.updates)
}

func (r *roster) join(m Member) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return applyUpdate(r.updates, newJoin(m))
}

func (r *roster) leave(id uuid.UUID, version int) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return applyUpdate(r.updates, newLeave(id, version))
}

func (r *roster) fail(id uuid.UUID, version int) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return applyUpdate(r.updates, newFail(id, version))
}

// A basic randomized iterator.  The goal of this is to avoid
// returning revoked members, while still guaranteeing that active
// members are visited at least once for every 2 complete iterations
type iterator struct {
	roster *roster
	order  []uuid.UUID
	idx    int
}

func NewIterator(roster *roster) *iterator {
	return &iterator{
		roster: roster,
		order:  shuffleIds(updatesToIds(roster.log()))}
}

func (i *iterator) Next() Member {
	idx := i.idx
	defer i.SetIndex(idx)

	if idx == len(i.order) {
		return nil
	}

	for ; idx < len(i.order); idx++ {
		member := i.roster.Get(i.order[idx])
		if member != nil {
			return member
		}
	}

	return nil
}

func (i *iterator) SetIndex(val int) {
	i.idx = val
}

func updateToMember(u update) Member {
	switch t := u.(type) {
	case *leave:
		return nil
	case *join:
		return t.member
	}

	panic("Unknown update type!")
}

func indexedUpdatesToUpdates(index map[uuid.UUID]update) []update {
	values := make([]update, 0, len(index))
	for _, v := range index {
		values = append(values, v)
	}

	return values
}

func updatesToIds(updates []update) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(updates))
	for _, u := range updates {
		ids = append(ids, u.Re())
	}

	return ids
}

func shuffleIds(ids []uuid.UUID) []uuid.UUID {
	perm := rand.Perm(len(ids))

	ret := make([]uuid.UUID, len(ids))
	for i, idx := range perm {
		ret[i] = ids[idx]
	}

	return ret
}

func applyUpdate(init map[uuid.UUID]update, u update) bool {
	memberId := u.Re()

	cur := init[memberId]
	if cur == nil {
		init[memberId] = u
		return true
	}

	if cur.Version() < u.Version() {
		init[memberId] = u
		return true
	}

	if _, ok := u.(*leave); ok {
		init[memberId] = u
		return true
	}

	return false
}

type fail struct {
	memberId uuid.UUID
	version  int
}

func (f *fail) Re() uuid.UUID {
	return f.memberId
}

func (f *fail) Version() int {
	return f.version
}

func (f *fail) Apply(r Roster) bool {
	return r.fail(f.memberId, f.version)
}

type leave struct {
	memberId uuid.UUID
	version  int
}

func (d *leave) Re() uuid.UUID {
	return d.memberId
}

func (d *leave) Version() int {
	return d.version
}

func (d *leave) Apply(r Roster) bool {
	return r.leave(d.memberId, d.version)
}

type join struct {
	member Member
}

func (p *join) Re() uuid.UUID {
	return p.member.Id()
}

func (p *join) Version() int {
	return p.member.Version()
}

func (p *join) Apply(r Roster) bool {
	return r.join(p.member)
}
