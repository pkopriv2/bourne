package kayak

import "time"

// Public Configuration
var Config = struct {

	// The key for specifying request timeout
	RequestTimeout string
	RequestTimeoutDefault time.Duration

	// The key for setting the base election timeout ()
	BaseElectionTimeout string
	BaseElectionTimeoutDefault time.Duration

	// The number of workers that operate in the server pool.
	ServerWorkers string
	ServerWorkdersDefault int

	// The maximum number of connections to allows to the leader
	LeaderPool string
	LeaderPoolDefault int

	// The key for specifying request timeout
	ConnectionTimeout string
	ConnectionTimeoutDefault time.Duration

	// The key for specifying the storage location
	StoragePath string
	StoragePathDefault string
}{
	"kayak.request.timeout",
	10 * time.Second,

	"kayak.base.election.timeout",
	2 * time.Second,

	"kayak.server.workers",
	50,

	"kayak.leader.connection.pool",
	10,

	"kayak.connection.timeout",
	15 * time.Second,

	"kayak.storage.path",
	"/var/kayak/storage.db",
}
