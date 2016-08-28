package msg

import "sync"
//import "sync/atomic"
import "io"
//import "log"
import "errors"

// A multiplexer is responsible for taking a single data stream
// and splitting it into multiple logical streams.  Once split,
// these streams are referred to as "channels".  Channels represent
// one side of a conversation between two entities.  Channels come
// in two different flavors:
//
//      * Active - An active channel represnts a "live" conversation
//        between two entities.  Channels are full duplex, meaning that they
//        have seaprate input and output streams.  The realationship between
//        client and server is NOT specified at this layer, but it is generally
//        perceived that a listening channel will spawn a "server" channel
//        while client channels must be spawned adhoc via the connect command.
//
//      * Listening - A listening channel spawns active channels.
//
// Data flow:
//
//  IN FLOW:
//  ---> RAW IN STREAM ---> PARSER ---> ROUTER ----> *CHANNEL
//
//  OUT FLOW:
//  ---> *CHANNEL ---> PARSER ----> *CHANNEL
//
// NOTE: Any reference to output refers to the OUT FLOW direction.
//
// Concurrency model:
//
//      * RAW IN STREAM  : SINGLE THREADED (No one but multiplexer should read from this)
//      * RAW OUT STREAM : SINGLE THREADED (No one but multiplexer should write to this)
//      * PARSER         : A single instance operating in its own routine (buffered output)
//      * ROUTER         : A single instance operating in its own routine.(buffered input, output)
//                         Eventually, it will be best to have many router instances that receive
//                         data based on a consistent hash of the dst channel address.
//      * CHANNEL        : Operates in its own routine (buffered input)
//
// Channel closing:
//
//      * Channel closing operates in the OUT direction and terminates
//
// Multiplex closing:
//
//      * Multiplex closing operates first in the OUT direction
//
// Example Client:
//   m := NewMultiplexer(...)
//
// Example:
//
// This is a poor error
var CHANNEL_EXISTS_ERROR = errors.New("Channel exists!")

// "ACTIVE" channels will tend to downward.
var CHANNEL_ACTIVE_MAX_ID uint16 = 65535
var CHANNEL_ACTIVE_MIN_ID uint16 = 256

// "LISTENING" channels will be in the range [0,255]
var CHANNEL_LISTEN_MIN_ID uint16 = 0
var CHANNEL_LISTEN_MAX_ID uint16 = 255

var CHANNEL_BUF_IN_SIZE uint = 1024

var ROUTER_BUF_IN_SIZE  uint = 8192
var ROUTER_BUF_OUT_SIZE uint = 8192

var PARSER_BUF_OUT_SIZE uint = 1024

var CONNECTION_MAX_CONNECT_RETRIES uint = 5

// A channel is uniquely identified by the entity and the channel id
//
// This layer of abstraction does NOT implement any security concepts
// (e.g. authentication/authorization), but simply manages the lifecyles
// of channel streams.
//
type ChannelAddress struct { entityId uint32; channelId uint16 }

// The primary consumer abstraction. For listening channels, this function
// will be cloned for each spawned channel.  Handlers, must therefore be,
// thread-safe.  Consumers should take great care when closing over
// external variables.
//
// To close a channel, consumers simply return from the function.
//
type ChannelHandler func(r io.Reader, w io.Writer) error

// Stream factories are used to create the multiplexed streams.  In
// the event of failure, this allows streams to be "recreated", without
// leaking how the streams are generated.
//
type StreamFactory func() ( io.ReadWriter, error )

// The channel id pool is where ids will be generated. The pool will be
// restricted to the range defined by:
//
//  [CHANNEL_ACTIVE_MIN_ID, CHANNEL_ACTIVE_MAX_ID]
//
type ChannelIdPool struct {
    mutex sync.Mutex
    pool []uint16 // we'll just always pull the first
}

func NewChannelIdPool() {
    mutex := new(sync.Mutex)
    pool  := make([]uint16, 32)
}

// Returns the next id in the channel pool.  Blocks
// until one is available.
func (self* ChannelIdPool) Take() (uint16) {
    for {
        mutex.
    }

    panic()
}

// Returns an id to the pool
func (self* ChannelIdPool) Return(id uint16) {
    return 0
}

// A channel represents one side of a connection within the multiplexer.
//
// Channels come in two flavors.
//
//  * Active
//  * Listening (no remote)
//
type Channel struct {

    // the local channel address
    local ChannelAddress

    // the remote channel address. nil for a listener
    remote *ChannelAddress

    // the incoming packet stream
    in chan Packet

    // the outgoing incoming stream
    out chan Packet

    // the channel's lock (internal only!)
    mutex sync.RWMutex

    // a flag indicating that the channel is closed.
    closed bool

    // used to wait on all routines to exit.
    wait sync.WaitGroup
}

// Closes the channel.  Expect all further calls to fail.
//
func (self *Channel) Close() error {
    self.mutex.Lock(); defer self.mutex.Unlock()

    // close the inner channels.
    close(self.in)
    close(self.out)

    // wait for the routine to close
    wait.Done();

    // mark the channel as closed
    self.closed = true
}

// Spawns a server channel.  A server differs from a client channel in that
// only a client may initiate communication.  This must accept a request
// before any activity is done.
//
func SpawnServerChannel(l ChannelAddress, in chan Packet, out chan<- Packet, fn ChannelHandler) *ServerChannel {
    readerChan := make(chan Packet)
    writerChan := make(chan Packet)

    reader := NewPacketReader(readerChan)
    writer := NewPacketWriter(writerChan,  )

    go func(in chan Packet, out chan<- Packet) {
        for {
            packet = <-Packet
        }
    }(l,r,in,out,fn)

    return &Channel { l, r, in }
}


type Channels struct {
    lock sync.RWMutex
    channels map[ChannelAddress]*Channel
}

func (s *channels) get(entityId uint32, channelId uint16) *Channel {
    s.lock.RLock(); defer s.lock.RUnlock()
    return s.channels[ChannelAddress{entityId, channelId}]
}

func (s *channels) del(entityId uint32, channelId uint16) error {
    s.lock.Lock(); defer s.lock.Unlock()

    addr := ChannelAddress{entityId, channelId}

    ret := s.channels[addr]
    delete(s.channels, addr)
    return ret
}

func (s *channels) add(c *Channel) error {
    s.lock.Lock(); defer s.lock.Unlock()

    ret := s.channels[c.local]
    if ret != nil {
        return CHANNEL_EXISTS_ERROR
    }

    s.channels[c.local] = c
    return nil;
}

func (s *Channels) list() map[ChannelAddress]*Channel {
    s.lock.RLock(); defer s.lock.RUnlock()
    return s.sessions // this assumes a copy by
}

// The "end of the line" for any bourne datagram.
//
// This is the primary routing from an underlying stream to the higher
// level sessions (aka sessions)
//
type Multiplexer struct {
    // all the active routines under this multiplexer
    workers sync.WaitGroup

    // the shared outgoing channel.  All multiplex sessions write to this channel
    outgoing chan Packet

    // the complete listing of sessions (both active and listening)
    sessions Channels
}

func (self *Multiplexer) Connect(srcEntityId uint32, dstEntityId uint32, dstChannelId uint16, fn ChannelHandler) (error) {
    return nil, nil
}

func (self *Multiplexer) Listen(srcEntityId uint32, srcChannelId uint16, fn ChannelHandler) (error) {
    return nil, nil
}

func (self *Multiplexer) Close() (error) {
    return nil
}

//// Creates a new multiplexer over the reader and writer
////
//// NOTE: the reader and writer must be exclusive to this
//// multiplexer.  We cannot allow interleaving of messages
//// on the underlying stream.
////
//// TODO: accept a connection factory so that we can make
//// a highly reliable multiplexer that can reconnect automatically
//// if any errors occur on the underlying stream
////
//func NewMultiplexer(r io.Reader, w io.Writer) *Multiplexer {

    //// initialize the wait group
    //workers := sync.WaitGroup{}

    //// create the shared write channel.
    //outgoing := make(chan Packet)

    //// initialize the channel map
    //var sessions Channels

    ////// create the writer routine
    ////go func(outgoing chan Packet) {
        ////log.Println("Starting the writer routine")
        ////workers.Add(1)

        ////for {
            ////msg := <-outgoing
            ////msg.write(w)
        ////}

        ////workers.Done()
    ////}(outgoing)

    ////// create the reader routine.
    ////go func() {
        ////log.Println("Starting the reader routine")
        ////workers.Add(1)

        ////for {
            ////msg, err := readPacket(r)
            ////if err != nil {
                ////log.Printf("Error reading from stream [%v]\n", err)
                ////continue
            ////}

            ////if err := handleIncoming(&sessions, &listening, msg); err != nil {
                ////log.Printf("Error handling message [%v]\n", err)
                ////continue
            ////}
        ////}

        ////workers.Done()
    ////}()

    //// okay, the multiplexer is ready to go.
    //return &Multiplexer { workers, outgoing, sessions }
//}

//func msgReader(reader *io.Reader, writer *io.Writer) {

//}
////func handleIncoming(sessionsChannels *ChannelLocks, listeningChannels *ChannelLocks, msg *Packet) (error) {
    ////id := ChannelId{msg.dstEntityId, msg.dstChannelId}

    ////// see if an active channel should handle this message
    ////activeChannels.RLock(); defer activeChannels.RUnlock()
    ////if c := activeChannels.data[id]; c != nil {
        ////log.Printf("There is an active channel: ", c)
        ////return;
    ////}

    ////// we still have to unlock
    ////activeChannels.RUnlock();

    ////listeningChannels.RLock(); defer listeningChannels.RUnlock()
    ////if c := listeningChannels.data[id]; c != nil {
        ////log.Printf("There is an listening channel: ", c)
    ////}
    ////// no active channel.  see if a channel was listening.

    ////return nil
////}


////type PacketProcessor interface {
    ////Process(msg Packet) (bool, error)
////}

////type PacketProcesorChain struct {
    ////next *PacketProcessor
////}
