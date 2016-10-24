package convoy

import (
	"sync"

	"github.com/pkopriv2/bourne/concurrent"
)

type peer struct {
	roster  Roster
	updates Updates
	diss    Disseminator
	pool    concurrent.WorkPool

	closer chan struct{}
	closed chan struct{}
	wait   sync.WaitGroup
}

func (p *peer) Roster() Roster {
	return p.roster
}

func (p *peer) Update(u update) bool {
	ret := u.Apply(p.roster)
	p.updates.Push() <- pending{u, 0}
	return ret
}

// func sendUpdate(u Update, t time.Duration) func() {
// return func(resp chan<- interface{}) {
// }
// }
