package convoy

import (
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// References:
//  * Mathematical Analysis: http://se.inf.ethz.ch/old/people/eugster/papers/gossips.pdf
//  * Mathematical Analysis: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.557.1902&rep=rep1&type=pdf
//  * https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
//  * https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
//  * Basic Design: http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf
//

// A member represents the fundamental unit of identity within a group.
type Member interface {
	Id() uuid.UUID
	Conn() (net.Connection, error)
	Version() int
	client() (client, error)
	serialize() interface{}
}

// A service exposes the standard actions to be taken on members.
// For the purposes of inventory management, these will provide
// the standard ping and proxy ping actions.
type client interface {
	Close() error
	Ping() (bool, error)
	PingProxy(uuid.UUID) (bool, error)
	Update([]update) ([]bool, error)
}

// A peer is a service that hosts the distributed roster.  A group
// of peers use a simple gossip protocol to distribute updates to
// each other.  A word regarding implementation:  Updates to a
// peer MUST originate from the member they are addressed to update
// or at least must coordinate with the member to determine an
// appropriate version for the data!  Otherwise, inconsisentcies
// can and most likely WILL happen!!!!!
type Peer interface {
	Close() error
	Roster() Roster
	ping(uuid.UUID) (bool, error)
	update([]update) ([]bool, error)
}

// the primary reconciliation technique will involve a "globally unique"
// counter for each member.  Luckily, we can distribute the counter to
// the members themselves, allowing us no consistency issues.
type clock interface {
	Cur() int
	Inc() int
}

// An update is the basic unit of change.  In practical terms, an update
// is either a put or a delete.
type update interface {
	Re() uuid.UUID
	Version() int
	Apply(Roster) bool
}

// The roster is the database of members.  The roster can be obtained
// via a single peer.
type Roster interface {
	// Returns the number of active members of the group.
	Size() int

	Get(uuid.UUID) Member

	// Returns an iterator that provides a random permutation over the
	// current roster members.  Returns nil once all current members
	// have been iterated.  Implementations should guarantee that
	// at most 2 complete iterations are required to visit every member
	// in the event of concurrent updates to the roster
	Iterator() Iterator

	join(Member) bool
	leave(uuid.UUID, int) bool
	// NOTE: leaving failed status out for now...not sure if it's necessary!
	// fail(Member) bool
	log() []update
}

type Iterator interface {
	Next() Member
}
