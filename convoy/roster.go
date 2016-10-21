package convoy

import (
	"math/rand"
	"sync"

	uuid "github.com/satori/go.uuid"
)

func newPut(member Member) Update {
	return &put{member}
}

func newDelete(memberId uuid.UUID, version int) Update {
	return &delete{memberId, version}
}

type roster struct {
	lock    sync.RWMutex
	updates map[uuid.UUID]Update
}

func newRoster() Roster {
	return &roster{updates: make(map[uuid.UUID]Update)}
}

func (r *roster) Iterator() Iterator {
	return NewIterator(r)
}

func (r *roster) Get(id uuid.UUID) Member {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return updateToMember(r.updates[id])
}

func (r *roster) log() []Update {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return indexedUpdatesToUpdates(r.updates)
}

func (r *roster) put(m Member) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return applyUpdate(r.updates, newPut(m))
}

func (r *roster) del(id uuid.UUID, version int) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return applyUpdate(r.updates, newDelete(id, version))
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

func updateToMember(u Update) Member {
	switch t := u.(type) {
	case *delete:
		return nil
	case *put:
		return t.member
	}

	panic("Unknown update type!")
}

func indexedUpdatesToUpdates(index map[uuid.UUID]Update) []Update {
	values := make([]Update, 0, len(index))
	for _, v := range index {
		values = append(values, v)
	}

	return values
}

func updatesToIds(updates []Update) []uuid.UUID {
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

func applyUpdate(init map[uuid.UUID]Update, u Update) bool {
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

	if _, ok := u.(*delete); ok  {
		init[memberId] = u
		return true
	}

	return false
}

type delete struct {
	memberId uuid.UUID
	version  int
}

func (d *delete) Re() uuid.UUID {
	return d.memberId
}

func (d *delete) Version() int {
	return d.version
}

func (d *delete) Apply(r Roster) bool {
	return r.del(d.memberId, d.version)
}

type put struct {
	member Member
}

func (p *put) Re() uuid.UUID {
	return p.member.Id()
}

func (p *put) Version() int {
	return p.member.Version()
}

func (p *put) Apply(r Roster) bool {
	return r.put(p.member)
}
