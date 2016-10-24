package convoy

import "time"

const (
	confPingTimeout   = "convoy.ping.timeout"
	confUpdateTimeout = "convoy.update.timeout"
)

const (
	defaultPingTimeout   = time.Second
	defaultUpdateTimeout = time.Second
)
