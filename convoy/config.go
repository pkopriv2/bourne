package convoy

import "time"

const (
	confPingTimeout         = "convoy.ping.timeout"
	confUpdateTimeout       = "convoy.update.timeout"
	confUpdateBatchSize     = "convoy.update.batch.size"
	confDisseminationPeriod = "convoy.dissemination.period"
	confServerPoolSize      = "convoy.server.pool.size"
)

const (
	defaultPingTimeout         = time.Second
	defaultUpdateTimeout       = time.Second
	defaultUpdateBatchSize     = 50
	defaultDisseminationPeriod = 5 * time.Second
	defaultServerPoolSize      = 10
)
