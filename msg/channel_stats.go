package msg

import (
	"fmt"

	metrics "github.com/rcrowley/go-metrics"
)

type ChannelStats struct {
	// track packet stats
	packetsSent     metrics.Counter
	packetsDropped  metrics.Counter
	packetsReceived metrics.Counter

	// track byte states
	bytesSent     metrics.Counter
	bytesDropped  metrics.Counter
	bytesReceived metrics.Counter
	bytesReset    metrics.Counter

	// track byte states
	numResets metrics.Counter
}

func NewChannelStats(addr ChannelAddress) *ChannelStats {
	r := metrics.DefaultRegistry

	return &ChannelStats{
		packetsSent: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.PacketsSent"), r),
		packetsReceived: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.PacketsReceived"), r),
		packetsDropped: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.PacketsDropped"), r),

		bytesSent: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.BytesSent"), r),
		bytesReceived: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.BytesReceived"), r),
		bytesDropped: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.BytesDropped"), r),
		bytesReset: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.BytesReset"), r),
		numResets: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.NumResets"), r)}
}

func NewChannelMetricName(addr ChannelAddress, name string) string {
	return fmt.Sprintf("-- %+v --: %s", addr, name)
}
