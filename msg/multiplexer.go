package msg

import "sync"
import "sync/atomic"
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

//
var CONNECTION_REFUSED_ERROR = errors.New("Connection refused")

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

//
//
type PacketProcessor interface {

    //
    Send(p *Packet) error

    //
    Close() error
}

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
    lock *sync.Mutex
    avail *list.List
    next uint16 // used as a low watermark
}

// Creates a new id pool.  The pool is initialized with
// ID_POOL_EXP_INC values.  Each time the pool's values
// are exhausted, it is automatically and safely expanded.
//
func NewIdPool() *IdPool {
    lock := new(sync.Mutex)
    avail := list.New()

    pool := &IdPool{ lock, avail, ID_POOL_MAX_ID }
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
    self.lock.Lock(); defer self.lock.Unlock()

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
    self.lock.Lock(); defer self.lock.Unlock()
    if id < self.next {
        panic("Returned an invalid id!")
    }

    self.avail.PushFront(id)
}

// A channel represents one side of a connection within the multiplexer.
//
// *This object is thread safe.*
//
type Channel struct {

    // the local channel address
    local ChannelAddress

    // the remote channel address
    remote ChannelAddress

    // the packet->stream reader
    reader io.Reader

    // the stream->packet writer
    writer io.Writer

    // the buffered "input" channel (owned by this channel)
    buf chan Packet

    // the wait group for all underlying threads.
    worker sync.WaitGroup

    // a flag indicating that the channel is closed. (atomic bool)
    closed uint32
}

// Creates and returns a new channel.
//
func NewChannel(l ChannelAddress, r ChannelAddress, out chan Packet) *Channel {
    // buffered input chan
    buf := make(chan Packet, CHANNEL_BUF_IN_SIZE)

    // synchronous reader chan
    syn := make(chan Packet)

    // io abstractions
    reader := NewPacketReader(syn)
    writer := NewPacketWriter(out, l.entityId, l.channelId, r.entityId, r.channelId)

    c := &Channel { l, r, reader, writer, buf, *new(sync.WaitGroup), 0}
    go func(c *Channel) {
        c.worker.Add(1)
        for {
            if c.isClosed() {
                defer close(c.buf)
                defer close(syn)
                return
            }
            syn<-(<-c.buf)
        }
        c.worker.Done()
    }(c)

    return c
}

// Returns whether this channel is closed.
//
func (self *Channel) isClosed() bool {
    return self.closed > 0
}

// Reads data from the channel.  Blocks if data
// isn't available.
//
func (self *Channel) Read(buf []byte) (int, error) {
    if self.isClosed() {
        return 0, CHANNEL_CLOSED_ERROR
    }

    return self.reader.Read(buf);
}

// Writes data to the channel.  Blocks if the underlying
// buffer is full.
//
func (self *Channel) Write(data []byte) (int, error) {
    if self.isClosed() {
        return 0, CHANNEL_CLOSED_ERROR
    }

    return self.writer.Write(data);
}

// Sends a packet to the channel stream.
//
func (self *Channel) Send(p *Packet) error {
    if self.isClosed() {
        return CHANNEL_CLOSED_ERROR
    }

    self.buf <- *p
    return nil
}

// Closes the channel.  Returns an error the if the
// channel is already closed.
//
func (self *Channel) Close() error {
    if atomic.CompareAndSwapUint32(&self.closed, 0, 1) {
        return CHANNEL_CLOSED_ERROR
    }

    self.reader = nil
    self.writer = nil
    self.worker.Wait()
    return nil
}

// // The second stage in the processing pipeline.
// //
// type ChannelRouter struct {
//
    // // the pool of ids (no need to take locks on this structure.)
    // pool IdPool
//
    // // the channel's lock (internal only!)
    // lock sync.RWlock
//
    // // channels map
    // channels map[ChannelAddress]*Channel
//
    // // the input channel
    // buf chan Packet
//
    // // wait
    // worker sync.WaitGroup
//
    // // a flag indicating that the channel is closed.
    // closed bool
//
// }
//
// // Creates and returns a new channel.
// //
// func NewChannelRouter(out chan Packet) *Channel {
    // router : = &ChannelRouter { l, r, *make(chan Packet, CHANNEL_BUF_IN_SIZE), *new(sync.WaitGroup), 0}
//
    // go func(c *ChannelRouter) {
        // c.worker.Add(1)
//
        // in := make(chan Packet)
        // reader := NewPacketReader(c.in)
        // writer := NewPacketWriter(out, l.entityId, l.channelId, r.entityId, r.channelId)
//
        // for {
            // if c.isClosed() {
                // defer close(c.in)
                // defer close(in)
                // return
            // }
//
            // in <- c.in
        // }
//
        // c.worker.Done()
    // }(c)
//
    // return c
// }
//
// // Closes the channel.  Returns an error the if the
// // channel is already closed.
// //
// func (self *ChannelRouter) Close() error {
    // self.lock.Lock(); defer self.lock.Unlock()
    // // set closed flag
//
    // self.closed = false
    // self.worker.Wait()
    // return nil
// }
//
// // Sends a packet to the channel stream.
// //
// func (self *ChannelRouter) Send(p *Packet) error {
    // self.lock.RLock(); defer self.lock.RUnlock()
    // if self.closed {
        // return CHANNEL_CLOSED_ERROR
    // }
//
    // self.buf <- *p
    // return nil
// }

//
// func (self *ChannelRouter) Start() error {
    // self.lock.RLock(); defer self.lock.RUnlock()
    // if self.closed {
        // return CHANNEL_CLOSED_ERROR
    // }
//
    // go func(router *ChannelRouter) {
        // for {
            // router.lock.RLock(); defer router.lock.RUnlock()
            // if router.closed {
                // break
            // }
//
            // p, ok := <-router.in
            // if ! ok {
                // router.Close()
            // }
//
            // channel := router.channels[ChannelAddress{p.dstEntityId, p.dstChannelId}]
            // if channel == nil {
                // router.out <- NewErrorPacket(p, CONNECTION_REFUSED_ERROR)
            // }
//
            // channel.Send(p)
        // }
    // }(self)
//
    // return nil
// }
//
// type PacketParser struct {
//
    // // a flag indicating that that the paris closed.
    // closed bool
//
    // // the channel's lock (internal only!)
    // lock sync.RWlock
//
    // // the "input" channel (owned by this channel)
    // in chan Packet
//
    // // the pool of ids
    // pool IdPool
//
    // // channels
    // channels map[ChannelAddress]*Channel
// }
//
// func (self *ChannelRouter) SpawnChannel(l ChannelAddress, r ChannelAddress) (error,  {
    // s.lock.Lock(); defer s.lock.Unlock()
    // channels[l] = NewChannel(l, r, make(chan Packet, CHANNEL_BUF_IN_SIZE))
// }
//
// func (self *ChannelRouter) Start() error {
    // s.lock.RLock(); defer s.lock.RUnlock()
    // if self.closed {
        // return CHANNEL_CLOSED_ERROR
    // }
//
    // go func(router *ChannelRouter) {
        // for {
            // lock.RLock(); defer s.lock.RUnlock()
//
            // channel := channels.Get(ChannelAddress{p.dstEntityId, p.dstChannelId})
            // if channel == nil {
                // router.out <- NewReturnPacket(p, CONNECTION_REFUSED_ERROR)
            // }
//
            // go func() {
                // channel.Send(p)
            // }
        // }
    // }(self)
//
    // return nil
// }
//
// // Spawns a new channel.
// //
// func (self *ChannelRouter) SpawnChannel(l ChannelAddress, r ChannelAddress) (error, *Channel)  {
    // self.lock.Lock(); defer self.lock.Unlock()
    // if self.closed {
        // return CHANNEL_CLOSED_ERROR
    // }
//
    // c := NewChannel(l, r, make(chan Packet, CHANNEL_BUF_IN_SIZE))
    // channels[l] = c
    // return c
// }
//
//
// // The "end of the line" for any bourne packet.
// //
// // This is the primary routing from an underlying stream to the higher
// // level sessions (aka sessions)
// //
// type Multiplexer struct {
//
    // // a multiplexer must be bound to a specific entity
    // entityId uint32
//
    // // a flag
    // closed bool
//
    // // the lock on the multiplexer
    // lock sync.RWlock
//
    // // all the active routines under this multiplexer
    // workers sync.WaitGroup
//
    // // the shared outgoing channel.  All multiplex sessions write to this channel
    // out chan Packet
//
    // // the complete listing of sessions (both active and listening)
    // channels *ChannelCache
//
    // // pool of available ids
    // pool *IdPool
//
// }
//
// func (self *Multiplexer) Start() () {
    // return nil, nil
// }
//
// func (self *Multiplexer) Connect(dstEntityId uint32, dstChannelId uint16) error {
    // return nil
// }
//
// func (self *Multiplexer) Listen(srcEntityId uint32, srcChannelId uint16) error {
    // return nil, nil
// }
//
// func (self *Multiplexer) Close() (error) {
    // return nil
// }
//
// //// Creates a new multiplexer over the reader and writer
// ////
// //// NOTE: the reader and writer must be exclusive to this
// //// multiplexer.  We cannot allow interleaving of messages
// //// on the underlying stream.
// ////
// //// TODO: accept a connection factory so that we can make
// //// a highly reliable multiplexer that can reconnect automatically
// //// if any errors occur on the underlying stream
// ////
// //func NewMultiplexer(r io.Reader, w io.Writer) *Multiplexer {
//
    // //// initialize the wait group
    // //workers := sync.WaitGroup{}
//
    // //// create the shared write channel.
    // //outgoing := make(chan Packet)
//
    // //// initialize the channel map
    // //var sessions ChannelCache
//
    // ////// create the writer routine
    // ////go func(outgoing chan Packet) {
        // ////log.Println("Starting the writer routine")
        // ////workers.Add(1)
//
        // ////for {
            // ////msg := <-outgoing
            // ////msg.write(w)
        // ////}
//
        // ////workers.Done()
    // ////}(outgoing)
//
    // ////// create the reader routine.
    // ////go func() {
        // ////log.Println("Starting the reader routine")
        // ////workers.Add(1)
//
        // ////for {
            // ////msg, err := readPacket(r)
            // ////if err != nil {
                // ////log.Printf("Error reading from stream [%v]\n", err)
                // ////continue
            // ////}
//
            // ////if err := handleIncoming(&sessions, &listening, msg); err != nil {
                // ////log.Printf("Error handling message [%v]\n", err)
                // ////continue
            // ////}
        // ////}
//
        // ////workers.Done()
    // ////}()
//
    // //// okay, the multiplexer is ready to go.
    // //return &Multiplexer { workers, outgoing, sessions }
// //}
//
// //func msgReader(reader *io.Reader, writer *io.Writer) {
//
// //}
// ////func handleIncoming(sessionsChannelCache *ChannelLocks, listeningChannelCache *ChannelLocks, msg *Packet) (error) {
    // ////id := ChannelId{msg.dstEntityId, msg.dstChannelId}
//
    // ////// see if an active channel should handle this message
    // ////activeChannelCache.RLock(); defer activeChannelCache.RUnlock()
    // ////if c := activeChannelCache.data[id]; c != nil {
        // ////log.Printf("There is an active channel: ", c)
        // ////return;
    // ////}
//
    // ////// we still have to unlock
    // ////activeChannelCache.RUnlock();
//
    // ////listeningChannelCache.RLock(); defer listeningChannelCache.RUnlock()
    // ////if c := listeningChannelCache.data[id]; c != nil {
        // ////log.Printf("There is an listening channel: ", c)
    // ////}
    // ////// no active channel.  see if a channel was listening.
//
    // ////return nil
// ////}
//
//
// ////type PacketProcessor interface {
    // ////Process(msg Packet) (bool, error)
// ////}
//
// ////type PacketProcesorChain struct {
    // ////next *PacketProcessor
// ////}
