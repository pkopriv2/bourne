package msg

import "sync"
import "io"
import "log"
import "errors"

// "ACTIVE" channels will grow downward
var ACTIVE_CHANNEL_MAX_ID uint16 = 65535
var ACTIVE_CHANNEL_MIN_ID uint16 = 256

// "LISTENING" channels will grow upward
var LISTEN_CHANNEL_MIN_ID uint16 = 0
var LISTEN_CHANNEL_MAX_ID uint16 = 255

type MultiplexChannel struct {
    // the "local" address of the channel
    localEntityId uint32
    localChannelId uint16

    // the "remote" address of the channel.  will be nil if this is a listening channel
    remoteEntityId *uint32
    remoteChannelId *uint16

    // the primary read channel.
    readChannel <-chan Datagram

    // the primary write channel (this is the same channel as the multiplex write channel)
    writeChannel chan<- Datagram
}

func (self *MultiplexChannel) Read() ([]byte, error) {
    //msg := <-self.readChannel
    return nil, nil
}

func (self *MultiplexChannel) Write(data []byte) (error) {
    return nil
}


// Unique channel address
//
type ChannelAddress struct {
    entityId uint32
    channelId uint16
}

type ChannelTracker struct {
    lock sync.RWMutex
    channel map[ChannelAddress]*MultiplexChannel
}

func (s *ChannelTracker) get(entityId uint32, channelId uint16) (*MultiplexChannel) {
    return nil;
}

func (s *ChannelTracker) del(entityId uint32, channelId uint16) (*MultiplexChannel) {
    return nil;
}

func (s *ChannelTracker) add(channel *MultiplexChannel) (error) {
    return nil;
}

func (s *ChannelTracker) list() ([]*MultiplexChannel) {
    return nil;
}


// The "end of the line" for any bourne datagram.
//
// At its core, this is simply a multiplexer over a raw byte stream
// This assumes the following guarantees of the underlying stream:
//
//  * Must be lossless
//
type Multiplexer struct {

    // all the active routines under this multiplexer
    workers sync.WaitGroup

    // the shared outgoing channel.  All multiplex channels write to this channel
    outgoing chan Datagram

    // the complete listing of channels
    channelsReqQueue chan MultiplexChannelRequest
    channelsResQueue chan MultiplexChannelRequest
}

// Creates a new multiplexer over the reader and writer
//
// NOTE: the reader and writer must be exclusive to this
// multiplexer.  We cannot allow interleaving of messages
// on the underlying stream.
//
// TODO: accept a connection factory so that we can make
// a highly reliable multiplexer that can reconnect automatically
// if any errors occur on the underlying stream
//
func NewMultiplexer(r io.Reader, w io.Writer) *Multiplexer {

    // initialize the wait group
    workers := sync.WaitGroup{}

    // create the shared write channel.
    outgoing := make(chan Datagram)

    // initialize the channel map
    var channels ChannelLocks

    // create the writer routine
    go func(outgoing chan Datagram) {
        log.Println("Starting the writer routine")
        workers.Add(1)

        for {
            msg := <-outgoing
            msg.write(w)
        }

        workers.Done()
    }(outgoing)

    // create the reader routine.
    go func() {
        log.Println("Starting the reader routine")
        workers.Add(1)

        for {
            msg, err := readDatagram(r)
            if err != nil {
                log.Printf("Error reading from stream [%v]\n", err)
                continue
            }

            if err := handleIncoming(&sessions, &listening, msg); err != nil {
                log.Printf("Error handling message [%v]\n", err)
                continue
            }
        }

        workers.Done()
    }()

    // okay, the multiplexer is ready to go.
    return &Multiplexer { workers, outgoing, channels }
}

func handleIncoming(sessionsChannels *ChannelLocks, listeningChannels *ChannelLocks, msg *Datagram) (error) {
    id := ChannelId{msg.dstEntityId, msg.dstChannelId}

    // see if an active channel should handle this message
    activeChannels.RLock(); defer activeChannels.RUnlock()
    if c := activeChannels.data[id]; c != nil {
        log.Printf("There is an active channel: ", c)
        return;
    }

    // we still have to unlock
    activeChannels.RUnlock();

    listeningChannels.RLock(); defer listeningChannels.RUnlock()
    if c := listeningChannels.data[id]; c != nil {
        log.Printf("There is an listening channel: ", c)
    }
    // no active channel.  see if a channel was listening.

    return nil
}

func (self *Multiplexer) Spawn(srcEntityId uint32, srcChannelId uint32, dstEntityId uint32, dstChannelId uint32) (*MultiplexChannel, error) {
    return nil, nil
}

func (self *Multiplexer) Listen(srcEntityId uint32, srcChannelId uint32) (*MultiplexerListener, error) {
    return nil, nil
}

func (self *Multiplexer) shutdown() (error) {
    return nil
}

type DatagramProcessor interface {
    Process(msg Datagram) (bool, error)
}

//type DatagramProcesorChain struct {
    //next *DatagramProcessor
//}

