package kayak

import "time"

// Public Configuration
var EnvConfig = struct {

	// The key for specifying request timeout
	RequestTimeout string

	// The key for setting the base election timeout ()
	BaseElectionTimeout string

	// The number of workers that operate in the server pool.
	ServerWorkers string
}{
	"kayak.request.timeout",
	"kayak.base.election.timeout",
	"kayak.server.pool.size",
}

var (
	defaultRequestTimeout      = 15 * time.Second
	defaultBaseElectionTimeout = 2 * time.Millisecond
)
