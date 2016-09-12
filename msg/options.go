package msg

import "time"

const (
	ChannelDefaultRecvInSize   = 1024
	ChannelDefaultRecvLogSize  = 1 << 20 // 1024K
	ChannelDefaultSendLogSize  = 1 << 18 // 256K
	ChannelDefaultSendWait     = 100 * time.Millisecond
	ChannelDefaultRecvWait     = 20 * time.Millisecond
	ChannelDefaultAckTimeout   = 5 * time.Second
	ChannelDefaultWinTimeout   = 2 * time.Second
	ChannelDefaultCloseTimeout = 10 * time.Second
	ChannelDefaultMaxRetries   = 3
)

// function used to configure a channel. (accepted as part of construction)
type ChannelConfig func(*ChannelOptions)

// function called on channel state changes.
type ChannelStateHandler func(Channel) error

// returns the default channel options struct
func DefaultChannelOptions() *ChannelOptions {
	return &ChannelOptions{
		RecvInSize:   ChannelDefaultRecvInSize,
		RecvLogSize:  ChannelDefaultRecvLogSize,
		SendLogSize:  ChannelDefaultSendLogSize,
		SendWait:     ChannelDefaultSendWait,
		RecvWait:     ChannelDefaultRecvWait,
		AckTimeout:   ChannelDefaultAckTimeout,
		WinTimeout:   ChannelDefaultWinTimeout,
		CloseTimeout: ChannelDefaultCloseTimeout,
		MaxRetries:   ChannelDefaultMaxRetries,

		// state handlers
		OnOpening: func(c Channel) error { return nil },
		OnOpen:    func(c Channel) error { return nil },
		OnClose:   func(c Channel) error { return nil },
		OnFailure: func(c Channel) error { return nil },
		OnData:    func(p *Packet) error { return nil }}
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

	// The duration to wait before the close is aborted.  any pending data is considered lost.
	CloseTimeout time.Duration

	// The number of consecutive retries before the channel is tarnsitioned to a failure state.
	MaxRetries uint

	// to be called when the channel has been initialized, and is in the process of being started
	OnOpening ChannelStateHandler

	// to be called when the channel has encountered an unrecoverable error
	OnOpen ChannelStateHandler

	// to be called when the channel is closed (allows the release of external resources (ie ids, routing table))
	OnClose ChannelStateHandler

	// to be called when the channel is closed (allows the release of external resources (ie ids, routing table))
	OnFailure ChannelStateHandler

	// to be called when the channel produces an outgoing packet. (may block)
	OnData func(*Packet) error
}
