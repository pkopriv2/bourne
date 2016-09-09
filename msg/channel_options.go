package msg

import "time"

const (
	CHANNEL_DEFAULT_RECV_IN_SIZE  = 1024
	CHANNEL_DEFAULT_RECV_LOG_SIZE = 1 << 20 // 1024K
	CHANNEL_DEFAULT_SEND_LOG_SIZE = 1 << 18 // 256K
	CHANNEL_DEFAULT_SEND_WAIT     = 100 * time.Millisecond
	CHANNEL_DEFAULT_RECV_WAIT     = 20 * time.Millisecond
	CHANNEL_DEFAULT_ACK_TIMEOUT   = 1 * time.Second
	CHANNEL_DEFAULT_WIN_TIMEOUT   = 2 * time.Second
	DATALOG_LOCK_WAIT             = 5 * time.Millisecond
)

// function used to configure a channel. (accepted as part of construction)
type ChannelConfig func(*ChannelOptions)

// function called on channel state changes.
type ChannelStateFn func(*ChannelActive) error

// returns the default channel options struct
func DefaultChannelOptions() *ChannelOptions {
	return &ChannelOptions{
		RecvInSize:  CHANNEL_DEFAULT_RECV_IN_SIZE,
		RecvLogSize: CHANNEL_DEFAULT_RECV_LOG_SIZE,
		SendLogSize: CHANNEL_DEFAULT_SEND_LOG_SIZE,
		SendWait:    CHANNEL_DEFAULT_SEND_WAIT,
		RecvWait:    CHANNEL_DEFAULT_RECV_WAIT,
		AckTimeout:  CHANNEL_DEFAULT_ACK_TIMEOUT,
		WinTimeout:  CHANNEL_DEFAULT_WIN_TIMEOUT,
		LogWait:     DATALOG_LOCK_WAIT,

		// handlers
		OnInit:  func(c *ChannelActive) error { return nil },
		OnOpen:  func(c *ChannelActive) error { return nil },
		OnClose: func(c *ChannelActive) error { return nil },
		OnError: func(c *ChannelActive) error { return nil },
		OnData:  func(p *Packet) error { return nil }}
}

// options struct
type ChannelOptions struct {

	// Whether or not to enable debug logging.
	Debug bool

	// Defines how many packets will be buffered before blocking
	RecvInSize uint

	// Defines how many bytes will be buffered prior to being consumed (should always be greater than send buf)
	RecvLogSize uint

	// Defines how many bytes will be buffered prior to being consumed (should always be greater than send buf)
	SendLogSize uint

	// The duration to wait before trying to send data again (when none was available)
	SendWait time.Duration

	// The duration to wait before trying to fetch again (when none was avaiable)
	RecvWait time.Duration

	// The duration to wait for an ack before data is considered lost
	AckTimeout time.Duration

	// The duration to wait for an ack before data is considered lost
	WinTimeout time.Duration

	// The duration to wait on incremental writes to the log
	LogWait time.Duration

	// to be called when the channel has been initialized, and is in the process of being started
	OnInit ChannelStateFn

	// to be called when the channel has encountered an unrecoverable error
	OnOpen ChannelStateFn

	// to be called when the channel is closed (allows the release of external resources (ie ids, routing table))
	OnClose ChannelStateFn

	// to be called when the channel has encountered an unrecoverable error
	OnError ChannelStateFn

	// to be called when the channel produces an outgoing packet. (may block)
	OnData func(*Packet) error
}
