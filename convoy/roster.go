package convoy

import (
	"math/rand"
	"sync"

	uuid "github.com/satori/go.uuid"
)

// Building a roster is basically indexing a sequence of updates.
// There are currently NO limitations on the ordering of updates.
// Reconciliation manages

type roster struct {
	lock    sync.RWMutex
	updates map[uuid.UUID]Update
}

func NewRoster() Roster {
	return &roster{updates: make(map[uuid.UUID]Update)}
}

func (r *roster) Iterator() Iterator {
	return NewIterator(r)
}

func (r *roster) Updates() []Update {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return extractValues(r.updates)
}

func (r *roster) All() []Member {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return extractMembers(r.updates)
}

func (r *roster) Get(id uuid.UUID) Member {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return extractMember(r.updates[id])
}

func (r *roster) Apply(updates []Update) error {
	r.lock.RLock()
	defer r.lock.RUnlock()
	applyUpdates(r.updates, updates)
	return nil
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
		order:  shuffleIds(extractIds(roster.All()))}
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

func extractMember(update Update) Member {
	return update.Member()
}

func extractMembers(raw map[uuid.UUID]Update) []Member {
	ret := make([]Member, 0, len(raw))

	for _, update := range raw {
		member := update.Member()
		if member == nil {
			continue
		}

		ret = append(ret, member)
	}

	return ret
}

func extractIds(members []Member) []uuid.UUID {
	ret := make([]uuid.UUID, 0, len(members))

	for _, m := range members {
		ret = append(ret, m.Id())
	}

	return ret
}

func shuffleIds(ids []uuid.UUID) []uuid.UUID {
	perm := rand.Perm(len(ids))

	ret := make([]uuid.UUID, len(ids))
	for i, idx := range perm {
		ret[i] = ids[idx]
	}

	return ret
}

func applyUpdates(init map[uuid.UUID]Update, updates []Update) {
	for _, u := range updates {
		cur := init[u.MemberId()]
		if cur == nil {
			init[u.MemberId()] = u
			continue
		}

		if cur.Version() < u.Version() {
			init[u.MemberId()] = u
			continue
		}

		// in the event of a collision, deletes win
		if u.Type() == Del {
			init[u.MemberId()] = u
		}
	}
}

func extractKeys(raw map[uuid.UUID]Update) []uuid.UUID {
	keys := make([]uuid.UUID, 0, len(raw))
	for k, _ := range raw {
		keys = append(keys, k)
	}

	return keys
}

func extractValues(raw map[uuid.UUID]Update) []Update {
	values := make([]Update, 0, len(raw))
	for _, v := range raw {
		values = append(values, v)
	}

	return values
}

func copyRoster(m map[uuid.UUID]Update) map[uuid.UUID]Update {
	ret := make(map[uuid.UUID]Update)
	for k, v := range m {
		ret[k] = v
	}
	return ret
}
