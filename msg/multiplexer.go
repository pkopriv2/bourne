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

// Each channel is able to buffer up to a certain number
// of packets on its incoming stream.
var CHANNEL_BUF_IN_SIZE uint = 1024

// The router will have the largest input buffer as it
// is paramount to not stop reading from the network
var ROUTER_BUF_IN_SIZE  uint = 8192

// the parser output buffer is shared amonst all channels
var PARSER_BUF_OUT_SIZE uint = 8192

// In order to build a highly resilient multiplexer we will
// impement reconnect logic.
var STREAM_MAX_RECONNECTS uint = 5


// Stream factories are used to create the underlying streams.  In
// the event of failure, this allows streams to be "recreated", without
// leaking how the streams are generated.  The intent is to create a
// highly resilient multiplexer.
//
// Consumers MUST NOT interfere with stream lifecycle.  Rather, they
// should manage the lifecycle of the multiplexer.
//
// TODO: Figure out how to initialize streams.
//
type StreamFactory func(entityId uint32) ( *io.ReadWriter, error )


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
