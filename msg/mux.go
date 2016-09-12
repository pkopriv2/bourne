package msg

import "sync"
import "io"
import "log"

// import "errors"

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
//  <DATA> ---> DESERIALIZER ---> ROUTER ----> *CHANNEL
//
//  OUT FLOW:
//  <DATA> ---> *CHANNEL ---> SERIALIZER ----> <DATA>
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
//      * CHANNEL      : Manages its own threds.
//
//
// Multiplex closing:
//
//      * Invoke close on all channels
//      * Closes the Reader
//      * Closes the Writer
//
// Examples:
//   m := NewMux(...)
//   s := m.connect(0,0)
//
// Example:
//

// the parser output buffer is shared amonst all channels
// var PARSER_BUF_OUT_SIZE uint = 8192

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
type StreamFactory func(entityId uint32) (*io.ReadWriter, error)

//
type Mux struct {

	// pool of available channel ids
	ids *IdPool

	// the complete listing of sessions (both active and listening)
	channels *ChannelCache

	// a flag indicating state.
	closed bool

	// the lock on the multiplexer
	lock sync.RWMutex

	// the shared output channel.
	out chan Packet

	// all the active routines under this multiplexer
	workers sync.WaitGroup
}

func NewMux(reader io.Reader, writer io.Writer) *Mux {
	parserOut := make(chan Packet, 1024)
	routerIn := make(chan Packet, 1024)

	mux := &Mux{ids: NewIdPool(), channels: NewChannelCache()}

	// start the parser thread
	mux.workers.Add(1)
	go func(mux *Mux, r io.Reader, next chan Packet) {
		defer mux.workers.Done()
		for {

			packet, err := ReadPacket(r)
			if err != nil {
				log.Printf("Error parsing packet")
				continue
			}

			next <- *packet
		}
	}(mux, reader, routerIn)

	// start the writer thread
	mux.workers.Add(1)
	go func(mux *Mux, prev chan Packet, w io.Writer) {
		defer mux.workers.Done()
		for {
			packet, ok := <-prev
			if !ok {
				log.Println("Channel closed.  Stopping writer thread")
			}

			if err := WritePacket(w, &packet); err != nil {
				// TODO: Handle connection errors
				log.Printf("Error writing packet [%v]\n", err)
				continue
			}
		}
	}(mux, parserOut, writer)

	// start the packet router thread
	mux.workers.Add(1)
	go func(mux *Mux, prev chan Packet, err chan Packet) {
		defer mux.workers.Done()

		for {
			// p, ok := <-prev
			// if !ok {
				// // todo...return error packet!
				// return
			// }

			// // see if there is an "active" session
			// channel := mux.channels.get(newchanneladdress{p.dstentityid, p.dstchannelid})
			// if channel == nil {
				// continue
			// }
//
			// // see if there is a "listener" session
			// if err := channel.send(&p); err != nil {
				// // todo...return error packet!
				// continue
			// }
		}
	}(mux, routerIn, parserOut)

	return mux
}

func (m *Mux) Connect(srcEntityId uint32, dstEntityId uint32, dstChannelId uint16) (Channel, error) {
	return nil, nil
	// return NewActiveChannel(srcEntityId, ChannelAddress{dstEntityId, dstChannelId}, m.channels, m.ids, m.out)
}

func (m *Mux) Listen(srcEntityId uint32, srcChannelId uint16) (Listener, error) {
	return nil, nil
	// return NewChannelListener(&ChannelAddress{srcEntityId, srcChannelId}, m.channels, m.ids, m.out)
}

func (m *Mux) Close() error {
	return nil
}
