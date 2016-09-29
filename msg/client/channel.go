package client

import (
	"fmt"
	"io"
	"log"
	"time"

	"math/rand"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

const (
	ChannelOpeningErrorCode = 100
	ChannelClosingErrorCode = 101
	ChannelTimeoutErrorCode = 102
)

var (
	NewOpeningError = wire.NewProtocolErrorFamily(ChannelOpeningErrorCode)
	NewClosingError = wire.NewProtocolErrorFamily(ChannelOpeningErrorCode)
	NewTimeoutError = wire.NewProtocolErrorFamily(ChannelOpeningErrorCode)
)

const (
	ChannelInit = iota
	ChannelOpening
	ChannelOpened
	ChannelClosing
	ChannelClosed
)


type channelConfig struct {
	debug bool

	recvMax int

	ackTimeout   time.Duration
	closeTimeout time.Duration
	maxRetries   int
}

type ChannelOptions struct {
	Config utils.Config

	// lifecycle handlers
	OnInit  ChannelTransitionHandler
	OnOpen  ChannelTransitionHandler
	OnClose ChannelTransitionHandler
	OnFail  ChannelTransitionHandler
}

type ChannelOptionsHandler func(*ChannelOptions)

type ChannelTransitionHandler func(Channel) error

func defaultChannelOptions() *ChannelOptions {
	return &ChannelOptions{
		Config: utils.NewEmptyConfig(),

		// state handlers
		OnInit:  func(c Channel) error { return nil },
		OnOpen:  func(c Channel) error { return nil },
		OnClose: func(c Channel) error { return nil },
		OnFail:  func(c Channel) error { return nil }}
}

type channelComms struct {
	recvOut       chan []byte
	recvBuffer    chan []byte
	recvAssembler chan wire.SegmentMessage
	recvIn        chan wire.Packet

	sendIn        chan wire.Packet
	sendBuffer    chan []byte
	sendAssembler chan wire.SegmentMessage
	sendOut       chan []byte

	ack chan uint64

	onInit    ChannelTransitionHandler
	onOpen    ChannelTransitionHandler
	onClose   ChannelTransitionHandler
	onFailure ChannelTransitionHandler
}

type channel struct {

	// the complete address of the channel
	route wire.Route

	// general channel statistics
	stats *ChannelStats

	// the channel state machine
	machine utils.StateMachine

	// channel communications
	comms *channelComms

	// config
	config *channelConfig
}

// Creates and returns a new channel
func newChannel(route wire.Route, listening bool, opts ...ChannelOptionsHandler) *channel {
	// initialize the options.
	defaultOpts := defaultChannelOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}

	// defensively copy the options (this is to eliminate any reference to
	// the options that the consumer may have)
	options := *defaultOpts

	config := &channelConfig{
		debug:        options.Config.OptionalBool(confChannelDebug, false),
		ackTimeout:   options.Config.OptionalDuration(confChannelAckTimeout, defaultChannelAckTimeout),
		closeTimeout: options.Config.OptionalDuration(confChannelCloseTimeout, defaultChannelCloseTimeout),
		maxRetries:   options.Config.OptionalInt(confChannelMaxRetries, defaultChannelMaxRetries)}

	// create the channel channelComms
	comms := &channelComms{}

	// initialize the channel
	c := &channel{
		route:  route,
		stats:  newChannelStats(route.Src()),
		comms:  comms,
		config: config}

	// build the statemachine
	factory := utils.BuildStateMachine()
	// factory.AddState(ChannelInit, NewInitWorker(c))
	// factory.AddState(ChannelOpening, NewOpeningWorker(c, listening))
	// factory.AddState(ChannelOpened, NewRecvWorker(c), NewRecvAssembleWorker(c), NewRecvBufferWorker(c))

	// ugly....
	c.machine = factory.Start(ChannelInit)

	// finally, return it.
	return c
}

func (c *channel) Route() wire.Route {
	return c.route
}

// func (c *channel) Flush() error {
// return c.flush(365 * 24 * time.Hour)
// }

// Reads data from the channel.  Blocks if data isn't available.
func (c *channel) Read(buf []byte) (int, error) {
	return 0, nil
	// state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelFailure)
	// if state.Is(ChannelClosed | ChannelFailure) {
	// return 0, ErrChannelClosed
	// }
	//
	// return c.recvOut.Read(buf)
}

// Writes the data to the channel.  Blocks if the underlying send buffer is full.
func (c *channel) Write(data []byte) (int, error) {
	return 0, nil
	// state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelFailure)
	// if state.Is(ChannelClosed | ChannelClosing | ChannelFailure) {
	// return 0, ErrChannelClosed
	// }
	//
	// return c.sendIn.Write(data)
}

// Closes the channel.  Returns an error if the channel is already closed.
func (c *channel) Close() error {
	return nil
}

// ** INTERNAL ONLY METHODS **
func (c *channel) fail(reason error) {
	control, err := c.machine.Control()
	if err != nil {
		return
	}

	control.Fail(reason)
}

// Send pushes a message on the input channel.  (used for internal routing.)
func (c *channel) send(p wire.Packet) error {
	// c.recvIn <- p
	return nil
}

