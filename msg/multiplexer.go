package msg

import "sync"
//import "sync/atomic"
import "io"
import "container/list"
import "log"
import "errors"

// A multiplexer is responsible for taking a single data stream
// and splitting it into multiple logical streams.  Once split,
// these streams are referred to as "channels".  A channel represents
// one side of a conversation between two entities.  Channels come
// in two different flavors:
//
//      * Active - An active channel represents a "live" conversation
//        between two entities.  Channels are full duplex, meaning that they
//        have separate input and output streams.  The realationship between
//        client and server is NOT specified at this layer, but it is generally
//        perceived that a listening channel will spawn a "server" channel
//        while client channels must be spawned adhoc via the connect command.
//
//      * Listening - A listening channel spawns active channels.
//
// Data flow:
//
//  IN FLOW:
//  <DATA> ---> READER ---> DESERIALIZER ---> ROUTER ----> *CHANNEL
//
//  OUT FLOW:
//  <DATA> ---> *CHANNEL ---> SERIALIZER ----> WRITER
//
// NOTE: Any reference to output refers to the OUT FLOW direction.
//
// Concurrency model:
//
//      * READER       : SINGLE THREADED (No one but multiplexer should read from this)
//      * WRITER       : SINGLE THREADED (No one but multiplexer should write to this)
//      * SERIALIZER   : A single instance operating in its own routine (buffered output)
//      * DESERIALIZER : A single instance operating in its own routine.(buffered input)
//      * ROUTER       : A single instance operating in its own routine.(buffered input)
//      * CHANNEL      : Operates in its own routine (buffered input)
//
// Channel shutdown
//
//      * Closes the Channel's send
//
// Multiplex closing:
//
//      * Closes the Reader
//      * Closes all send channels in the IN direction
//      * Closes all send channels in the OUT direction
//      * Closes the Writer
//
// Examples:
//   m := NewMultiplexer(...)
//   s := m.connect(0,0)
//
// Example:
//
var CHANNEL_EXISTS_ERROR = errors.New("Channel exists")
var CHANNEL_CLOSED_ERROR = errors.New("Channel closed")

// "ACTIVE" channel id pool.
var ID_POOL_MAX_ID uint16    = 65535 // exclusive
var ID_POOL_MIN_ID uint16    = 256   // exclusive
var ID_POOL_EXP_INC uint16   = 10
var ID_POOL_CAP_ERROR  error = errors.New("Pool has reached capacity!")

// "LISTENING" channels MUST be in the range [0,255]
var CHANNEL_LISTEN_MIN_ID uint16 = 0
var CHANNEL_LISTEN_MAX_ID uint16 = 255

var CHANNEL_BUF_IN_SIZE uint = 1024

var ROUTER_BUF_IN_SIZE  uint = 8192
var ROUTER_BUF_OUT_SIZE uint = 8192

var PARSER_BUF_OUT_SIZE uint = 1024

var STREAM_MAX_RECONNECTS uint = 5

// Within a single multiplexer, a channel is uniquely identified by the
// entity and the channel id
//
// This layer of abstraction does NOT implement any security concepts
// (e.g. authentication/authorization), but simply manages the lifecyles
// of channel streams.
//
type ChannelAddress struct { entityId uint32; channelId uint16 }

// Stream factories are used to create the underlying streams.  In
// the event of failure, this allows streams to be "recreated", without
// leaking how the streams are generated.  The intent is to create a
// highly resilient multiplexer.
//
// Consumers MUST NOT interfere with stream lifecycle.  Rather, they
// should manage the lifecycle of the multiplexer.
//
type StreamFactory func(entityId uint32) ( *io.ReadWriter, error )

// A memory efficient pool of available ids. The pool will be
// restricted to the range defined by:
//
//  [ID_POOL_MIN_ID, ID_POOL_MAX_ID]
//
// By convention, the pool will grow downward, meaning higher ids are
// favored.  Unlike a sync.Pool, this class does not automatically
// clean up the available pool.  This is to prevent leaving ids
// unavailable in the event of a deallocation.
//
// This pool does NOT track ownership, which allows someone to return
// an id they did take themselves.  In that event, the same id may be
// given out at the same time, which breaks an invariant of the
// multiplexer.
//
// *This object is thread-safe*
//
type IdPool struct {
    mutex *sync.Mutex
    avail *list.List
    next uint16 // used as a low watermark
}

// Creates a new id pool.  The pool is initialized with
// ID_POOL_EXP_INC values.  Each time the pool's values
// are exhausted, it is automatically and safely expanded.
//
func NewIdPool() *IdPool {
    mutex := new(sync.Mutex)
    avail := list.New()

    pool := &IdPool{ mutex, avail, ID_POOL_MAX_ID }
    pool.expand(ID_POOL_EXP_INC)
    return pool
}

// WARNING: Not thread-safe.  Internal use only!
//
// Expands the available ids by numItems or until
// it has reached maximum capacity.
//
func (self* IdPool) expand(numItems uint16) error {
    log.Printf("Attemping to expand id pool [%v] by [%v] items\n", self.next, numItems)

    i, prev := self.next, self.next
    for ; i > prev - numItems && i >= ID_POOL_MIN_ID; i-- {
        self.avail.PushBack(i)
    }

    // if we didn't move i, the pool is full.
    if i == prev {
        return ID_POOL_CAP_ERROR
    }

    // move the watermark
    self.next = i
    return nil
}

// Takes an available id from the pool.  If one can't be taken
// a non-nil error is returned.
//
// In the event of a non-nil error, the consumer MUST not use the
// returned value.
//
func (self* IdPool) Take() (uint16, error) {
    self.mutex.Lock(); defer self.mutex.Unlock()

    // see if anything is available
    if item := self.avail.Front(); item != nil {
        return self.avail.Remove(item).(uint16), nil
    }

    // try to expand the pool
    if err := self.expand(ID_POOL_EXP_INC); err != nil {
        return 0, err
    }

    // okay, the pool has been expanded
    return self.avail.Remove(self.avail.Front()).(uint16), nil
}

// Returns an id to the pool.
//
// **WARNING:**
//
//  Only ids that have been loaned out should be returned to the
//  pool.
//
func (self* IdPool) Return(id uint16) {
    if id < self.next {
        panic("Returned an invalid id!")
    }

    self.mutex.Lock(); defer self.mutex.Unlock()
    self.avail.PushFront(id)
}

// A channel represents one side of a connection within the multiplexer.
//
// *This object is thread safe.*
//
type Channel struct {

    // IMMUTABLE FIELDS:

    // the local channel address
    local ChannelAddress

    // the remote channel address
    remote ChannelAddress

    // MUTABLE FIELDS:

    // a flag indicating that the channel is closed.
    closed bool

    // the channel's lock (internal only!)
    mutex sync.RWMutex

    // the stream reader
    reader io.Reader

    // the stream writer
    writer io.Writer

    // the "input" channel (owned by this channel)
    in chan Packet
}

// Creates and returns a new channel.
//
func NewChannel(l ChannelAddress, r ChannelAddress, out chan Packet) *Channel {
    in := make(chan Packet, CHANNEL_BUF_IN_SIZE)

    reader := NewPacketReader(in)
    writer := NewPacketWriter(out, l.entityId, l.channelId, r.entityId, r.channelId)
    mutex  := *new(sync.RWMutex)

    return &Channel { l, r, false, mutex, reader, writer, in }
}

// Returns the underlying reader.  Consumers are NOT expected
// to manage the lifecycle of the reader, but rather the channel
// itself.
//
func (self *Channel) Reader() io.Reader {
    return self.reader
}

// Returns the underlying writer.  Consumers are NOT expected
// to manage the lifecycle of the writer, but rather the channel
// itself.
//
func (self *Channel) Writer() io.Writer {
    return self.writer
}

// Returns the underlying writer.  Consumers are NOT expected
// to manage the lifecycle of the writer, but rather the channel
// itself.
//
func (self *Channel) Send(p *Packet) error {
    self.mutex.RLock(); defer self.mutex.RUnlock()
    if self.closed {
        return CHANNEL_CLOSED_ERROR
    }

    self.in <- *p
    return nil
}

// Closes the channel.  Returns an error the if the
// channel is already closed.
//
func (self *Channel) Close() error {
    self.mutex.Lock(); defer self.mutex.Unlock()
    if self.closed {
        return CHANNEL_CLOSED_ERROR
    }

    // close the streams
    // self.reader.Close()
    // self.writer.Close()

    // closes the reader input channel
    close(self.in)

    // cleanup the streams
    self.reader = nil
    self.writer = nil

    // mark the channel as closed
    self.closed = true
    return nil
}

type ChannelCache struct {
    lock sync.RWMutex
    channels map[ChannelAddress]*Channel
}

func (s *ChannelCache) Get(addr ChannelAddress) *Channel {
    s.lock.RLock(); defer s.lock.RUnlock()
    return s.channels[addr]
}

func (s *ChannelCache) Del(addr ChannelAddress) error {
    s.lock.Lock(); defer s.lock.Unlock()

    return nil
    // ret := s.channels[addr]
    // delete(s.channels, addr)
    // return ret
}

func (s *ChannelCache) Add(c *Channel) error {
    s.lock.Lock(); defer s.lock.Unlock()

    ret := s.channels[c.local]
    if ret != nil {
        return CHANNEL_EXISTS_ERROR
    }

    s.channels[c.local] = c
    return nil;
}

// func (s *ChannelCache) list() map[ChannelAddress]*Channel {
    // s.lock.RLock(); defer s.lock.RUnlock()
    // return s.channels // this assumes a copy by
// }

// The "end of the line" for any bourne packet.
//
// This is the primary routing from an underlying stream to the higher
// level sessions (aka sessions)
//
type Multiplexer struct {

    // a multiplexer must be bound to a specific entity
    entityId uint32

    // all the active routines under this multiplexer
    workers sync.WaitGroup

    // the shared outgoing channel.  All multiplex sessions write to this channel
    out chan Packet

    // the complete listing of sessions (both active and listening)
    channels ChannelCache

    //

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
    //var sessions ChannelCache

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
////func handleIncoming(sessionsChannelCache *ChannelLocks, listeningChannelCache *ChannelLocks, msg *Packet) (error) {
    ////id := ChannelId{msg.dstEntityId, msg.dstChannelId}

    ////// see if an active channel should handle this message
    ////activeChannelCache.RLock(); defer activeChannelCache.RUnlock()
    ////if c := activeChannelCache.data[id]; c != nil {
        ////log.Printf("There is an active channel: ", c)
        ////return;
    ////}

    ////// we still have to unlock
    ////activeChannelCache.RUnlock();

    ////listeningChannelCache.RLock(); defer listeningChannelCache.RUnlock()
    ////if c := listeningChannelCache.data[id]; c != nil {
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
