package msg

//import "sync"
//import "io"
//import "log"
import "errors"

// This is a poor error
var CHANNEL_EXISTS_ERROR = errors.New("Channel exists!")

// "ACTIVE" channels will grow downward
var ACTIVE_CHANNEL_MAX_ID uint16 = 65535
var ACTIVE_CHANNEL_MIN_ID uint16 = 256

// "LISTENING" channels will grow upward
var LISTEN_CHANNEL_MIN_ID uint16 = 0
var LISTEN_CHANNEL_MAX_ID uint16 = 255

// A session is uniquely identified by the entity and the work queue
//type ChannelAddress struct { entityId uint32; channelId uint16 }

//type ChannelHandler func(r io.Reader, w io.Writer) error

//type Channel struct {

    //// the local channel address
    //local ChannelAddress

    //// the remote channel address
    //remote ChannelAddress

    //// the incoming datagram stream
    //in chan Packet
//}

//func SpawnServerChannel(l ChannelAddress, r ChannelAddress, in chan Packet, out chan<- Packet, fn ChannelServerFn) *Channel {
    //// start the left channel
    //go func(in chan Packet, out chan<- Packet, fn ChannelServerFn) {
        //for {
        //}
    //}(l,r,in,out,fn)

    //return &Channel { l, r, in }
//}

//func (self *Channel) Send(msg Packet) error {
    //self.in<- msg
//}

//func (self *Channel) Close() error {
    //return nil
//}

//type Channels struct {
    //lock sync.RWMutex
    //channels map[ChannelAddress]*Channel
//}

//func (s *channels) get(entityId uint32, channelId uint16) *Channel {
    //s.lock.RLock(); defer s.lock.RUnlock()
    //return s.channels[ChannelAddress{entityId, channelId}]
//}

//func (s *channels) del(entityId uint32, channelId uint16) error {
    //s.lock.Lock(); defer s.lock.Unlock()

    //addr := ChannelAddress{entityId, channelId}

    //ret := s.channels[addr]
    //delete(s.channels, addr)
    //return ret
//}

//func (s *channels) add(c *Channel) error {
    //s.lock.Lock(); defer s.lock.Unlock()

    //ret := s.channels[c.local]
    //if ret != nil {
        //return CHANNEL_EXISTS_ERROR
    //}

    //s.channels[c.local] = c
    //return nil;
//}

//func (s *Channels) list() map[ChannelAddress]*Channel {
    //s.lock.RLock(); defer s.lock.RUnlock()
    //return s.sessions // this assumes a copy by
//}

//// The "end of the line" for any bourne datagram.
////
//// This is the primary routing from an underlying stream to the higher
//// level sessions (aka sessions)
////
//type Multiplexer struct {
    //// all the active routines under this multiplexer
    //workers sync.WaitGroup

    //// the shared outgoing channel.  All multiplex sessions write to this channel
    //outgoing chan Packet

    //// the complete listing of sessions (both active and listening)
    //sessions Channels
//}

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

////func (self *Multiplexer) Spawn(srcEntityId uint32, srcChannelId uint32, dstEntityId uint32, dstChannelId uint32) (*Channel, error) {
    ////return nil, nil
////}

////func (self *Multiplexer) Listen(srcEntityId uint32, srcChannelId uint32) (*MultiplexerListener, error) {
    ////return nil, nil
////}

//func (self *Multiplexer) shutdown() (error) {
    //return nil
//}

////type PacketProcessor interface {
    ////Process(msg Packet) (bool, error)
////}

////type PacketProcesorChain struct {
    ////next *PacketProcessor
////}
