package convoy

import (
	"fmt"
	"time"

	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// References:
//  * https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
//  * https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
//  * http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf
//
const (
	confPingTimeout   = "convoy.ping.timeout"
	confUpdateTimeout = "convoy.update.timeout"
)

const (
	defaultPingTimeout   = time.Second
	defaultUpdateTimeout = time.Second
)

type TimeoutError struct {
	timeout time.Duration
	message string
}

func (e TimeoutError) Error() string {
	return fmt.Sprintf("Timeout[%v]: %v", e.timeout, e.message)
}

type NoSuchMemberError struct {
	id uuid.UUID
}

func (e NoSuchMemberError) Error() string {
	return fmt.Sprintf("No such member [%v]", e.id)
}

// the primary reconciliation technique will involve a "globally unique"
// counter for each member.  Luckily, we can distribute the counter to
// the members themselves, allowing us no consistency issues.
type Clock interface {
	Cur() int
	Inc() int
}

// A member represents the fundamental unit of identity within a group.
// These will typically align with
type Member interface {
	Id() uuid.UUID
	Version() int
	Client() (Client, error)
}

// A service exposes the standard actions to be taken on members.
// For the purposes of inventory management, these will provide
// the standard ping and proxy ping actions.
type Client interface {
	Conn() net.Connection
	ping(time.Duration) (bool, error)
	pingProxy(uuid.UUID, time.Duration) (bool, error)
	update(Update, time.Duration) (bool, error)
}

// A peer is a service that hosts the distributed roster.  A group
// of peers use a simple gossip protocol to distribute updates to
// each other.  A word regarding implementation:  Updates to a
// peer MUST originate from the member they are addressed to update
// or at least must coordinate with the member to determine an
// appropriate version for the data!
type Peer interface {
	Roster() Roster
	update(Update) bool
}

// An update is the basic unit of change.  In practical terms, an
// update is either a put or a delete.
type Update interface {
	Re() uuid.UUID
	Version() int
	Apply(Roster) bool
}

// The roster is the database of members.  The roster can be obtained
// via a single peer.
type Roster interface {
	Get(uuid.UUID) Member

	// Returns an iterator that provides a random permutation over the
	// current roster members.  Returns nil once all current members
	// have been iterated.  Implementations should guarantee that
	// at most 2 complete iterations are required to visit every member
	// in the event of concurrent updates to the roster
	Iterator() Iterator

	put(Member) bool
	del(uuid.UUID, int) bool

	log() []Update
}

type Iterator interface {
	Next() Member
}

type PingRequest struct {
}

type ProxyPingRequest struct {
	Target uuid.UUID
}

type UpdateRequest struct {
	Update Update
}

type PingResponse struct {
}

type ProxyPingResponse struct {
	Success bool
	Err     error
}

type UpdateResponse struct {
	Success bool
}

type ErrorResponse struct {
	Err error
}
