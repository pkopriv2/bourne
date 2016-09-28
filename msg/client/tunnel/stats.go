package tunnel

import (
	"fmt"

	"github.com/pkopriv2/bourne/msg/wire"
	metrics "github.com/rcrowley/go-metrics"
)

type Stats struct {
	packetsSent     metrics.Counter
	packetsDropped  metrics.Counter
	packetsReceived metrics.Counter

	bytesSent     metrics.Counter
	bytesDropped  metrics.Counter
	bytesReceived metrics.Counter
	bytesReset    metrics.Counter

	numResets metrics.Counter
}

func newStats(endpoint wire.Address) *Stats {
	r := metrics.DefaultRegistry

	return &Stats{
		packetsSent: metrics.NewRegisteredCounter(
			newMetric(endpoint, "channel.wire.PacketsSent"), r),
		packetsReceived: metrics.NewRegisteredCounter(
			newMetric(endpoint, "channel.wire.PacketsReceived"), r),
		packetsDropped: metrics.NewRegisteredCounter(
			newMetric(endpoint, "channel.wire.PacketsDropped"), r),

		bytesSent: metrics.NewRegisteredCounter(
			newMetric(endpoint, "channel.BytesSent"), r),
		bytesReceived: metrics.NewRegisteredCounter(
			newMetric(endpoint, "channel.BytesReceived"), r),
		bytesDropped: metrics.NewRegisteredCounter(
			newMetric(endpoint, "channel.BytesDropped"), r),
		bytesReset: metrics.NewRegisteredCounter(
			newMetric(endpoint, "channel.BytesReset"), r),
		numResets: metrics.NewRegisteredCounter(
			newMetric(endpoint, "channel.NumResets"), r)}
}

func newMetric(endpoint wire.Address, name string) string {
	return fmt.Sprintf("-- %+v --: %s", endpoint, name)
}
