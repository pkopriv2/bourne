package convoy

import (
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// References:
//  * https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
//  * https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
//  * http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf
//
type UpdateType int

// the primary reconciliation technique will involve a "globally unique"
// counter for each member.  Luckily, we can distribute the counter to
// the members themselves, allowing us no consistency issues.
type Clock interface {
	Cur() int
	Inc() int
}

type Member interface {
	Id() uuid.UUID
	Conn() net.Connection
	Meta() map[string]string
}

type Iterator interface {
	Next() Member
}

const (
	Add UpdateType = 0
	Del UpdateType = 1
)

// Represents the fundamental unit of "change" in the system.
// The corresponding "roster" must
type Update interface {
	Type() UpdateType
	MemberId() uuid.UUID
	Version() int
	Member() Member // nil in the case
}

// The complete lsiting of members.
type Roster interface {
	Apply([]Update) error
	Updates() []Update

	// Returns an iterator that provides a random permutation over the
	// current roster members.  Returns nil once all current members
	// have been iterated.  Implementations should guarantee that
	// at most 2 complete iterations are required to visit every member
	// in the event of concurrent updates to the roster
	Iterator() Iterator
}

type Client interface {
	Leave()
	Ping(Member) bool
	PingReq([]Member) bool
}

func NewClient(c net.Connection) Client {
	return nil
}

type Updates []Updates

type Ping struct {
}

type PingRequest struct {
	MemberId uuid.UUID
}

type Ack struct {
}
