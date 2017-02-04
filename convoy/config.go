package convoy

import "time"

// Public Configuration
var Config = struct {

	// Storage path
	StoragePath string

	// The number of attempts to try to rejoin a cluster on failure.
	JoinAttempts string

	// The number of probes to send out to determine if a client is
	// truly unreachable.  In the event of that a member is no longer
	// reachable, they are deemed unhealthy and evicted from the cluster.
	HealthProbeCount string

	// The timeout used during health checks
	// See README.md  for more information on member health.
	HealthProbeTimeout string

	// The dissemination represents the number of ln(N) times a message
	// is redistributed.
	DisseminationFactor string

	// The dissemination period.
	DisseminationPeriod string

	// The disssemination batch size.  This along with the dissemination
	// period allows us to cap the overall network uses at each node
	// in events per period.
	DisseminationBatchSize string
}{
	"convoy.storage.path",
	"convoy.join.attempts.count",
	"convoy.health.probe.count",
	"convoy.health.probe.timeout",
	"convoy.dissemination.factor",
	"convoy.dissemination.period",
	"convoy.dissemination.size",
}

const (
	defaultStoragePath         = "/var/kayak/storage.db"
	defaultJoinAttemptsCount   = 10
	defaultHealthProbeTimeout  = 10 * time.Second
	defaultHealthProbeCount    = 3
	defaultDisseminationFactor = 3
	defaultDisseminationPeriod = 500 * time.Millisecond
	defaultServerPoolSize      = 20
)
