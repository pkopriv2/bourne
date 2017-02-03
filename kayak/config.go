package kayak

import "time"

// Public Configuration
var Config = struct {

	// The key for specifying request timeout
	RequestTimeout string

	// The key for setting the base election timeout ()
	BaseElectionTimeout string

	// The number of workers that operate in the server pool.
	ServerWorkers string

	// The maximum number of connections to allows to the leader
	LeaderPool string

	// The key for specifying request timeout
	ConnectionTimeout string
}{
	"kayak.request.timeout",
	"kayak.base.election.timeout",
	"kayak.server.workers",
	"kayak.leader.connection.pool",
	"kayak.connection.timeout",
}

var (
	defaultRequestTimeout       = 10 * time.Second
	defaultConnectionTimeout    = 15 * time.Second
	defaultBaseElectionTimeout  = 2 * time.Second
	defaultServerWorkers        = 50
	defaultLeaderConnectionPool = 10
)
