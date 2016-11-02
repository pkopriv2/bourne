package convoy

import (
	"github.com/pkopriv2/bourne/enc"
	uuid "github.com/satori/go.uuid"
)

// References:
//  * Mathematical Analysis: http://se.inf.ethz.ch/old/people/eugster/papers/gossips.pdf
//  * Mathematical Analysis: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.557.1902&rep=rep1&type=pdf
//  * https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
//  * https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
//  * Basic Design: http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf
//


// Joins the cluster
func JoinCluster(addr string) (Member, error) {
	return nil nil
}

func StartCluster(port int) (Member, error) {
	return nil, nil
}

func Connect(addr string) (Member, error) {
	return nil, nil
}


// A member represents the fundamental unit of identity within a group.
type Member interface {
	enc.Writable

	// Returns the id of the member
	Id() uuid.UUID

	// Returns the current version of the member (used during reconciliation)
	Version() int

	// Returns the standard services client.  Consumers must close the client.
	Client() (Client, error)
}


// A service exposes the standard actions to be taken on members.
// For the purposes of inventory management, these will provide
// the standard ping and proxy ping actions.
type Client interface {
	// Closes the client and frees up any resources.
	Close() error

	// A copy of the member's roster.
	Roster() Roster

	// Pings the member.  Returns true if the member is alive.
	Ping() (bool, error)

	// Requests that the member ping another another member.
	// True indicates the member was able to successfully ping
	// the target
	pingProxy(uuid.UUID) (bool, error)

	// Sends a batch of updates to the member.  Returns an array
	// indicating which updates were accepted
	update([]update) ([]bool, error)
}

type Store interface {
	Get(uuid.UUID) DataBase
	Add(uuid.UUID) DataBase
}

type DataBase interface {
	Get(key string) Entity
	NewUpdate()
}

type DataBase interface {
	Get(key string) Entity
	NewUpdate()
}

type Store interface {
	NewUpdate()
}

// The roster is the database of members.  The roster can be obtained via
// a single peer.
type Roster interface {

	// Returns the number of active members of the group.
	Size() int

	// Returns the member of the given id.   Only active members are returned.
	Get(uuid.UUID) Member

	// Returns an iterator that provides a random permutation over the
	// current roster members.  Returns nil once all current members
	// have been iterated.  Implementations should guarantee that
	// at most 2 complete iterations are required to visit every member
	// in the event of concurrent updates to the roster
	Iterator() Iterator

	// adds the member.  Returns true if the join was accepted.
	join(Member) bool

	// removes the member with the give id.  Returns true if the leave was accepted.
	leave(uuid.UUID, int) bool

	// Returns a raw log of updates that can be sent to other members.
	log() []update
}

type Iterator interface {
	Next() Member
}

// An update is the basic unit of change.  In practical terms, an update
// is either a put or a delete.
type update interface {
	enc.Writable

	// which member the update is in regards to.
	Re() uuid.UUID

	// the version that this update should apply to.  (used for reconciliation)
	Version() int

	// applys the update to the roster
	Apply(Roster) bool
}

