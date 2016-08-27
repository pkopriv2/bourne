package msg

import "io"
import "bytes"
import "encoding/binary"
//import "errors"

var PROTOCOL_VERSION uint16 = 0

// TODO: Research optimum packet sizes
//
var PACKET_MAX_DATA_LEN uint16 = 32767

// A multiplex packet is the base message that is
// routed amongst multiplex sessions
//
// As constructed, this assumes the following of the underlying stream:
//
//   -> reliable, in-order delivery (e.g. TCP)
//
// TODO: Add support for fragmentation.  We don't want to swamp a connection
// TODO: with a single packet.  As it's likely that many simultaneous
// TODO: conversations will be occurring.
type Packet struct {

    // necessary for backwards/forwards compatibility
    protocolVersion uint16

    // every packet must identify its source
    srcEntityId uint32
    srcChannelId uint16

    // every packet must identify its destination
    dstEntityId uint32
    dstChannelId uint16

    // control flags (used to control channel state)
    // [OPEN CLOSE ERROR etc..]
    //
    // TODO: research all the necessary control flags
    ctrls uint16

    // the raw data (to be interpreted by the consumer)
    data []uint8
}

// TODO: Consider using UVARINT encoding.  For now, sticking to fixed length encoding
func ReadPacket(r io.Reader) (*Packet, error) {
    // read all the header bytes
    headerBuf := make([]byte, 16)
    if _, err := io.ReadFull(r, headerBuf); err != nil {
        return nil, err
    }

    // read the data length bytes
    dataLenBuf := make([]byte, 2)
    if _, err := io.ReadFull(r, dataLenBuf); err != nil {
        return nil, err
    }

    // parse the length
    var dataLen uint16
    if err := binary.Read(bytes.NewReader(dataLenBuf), binary.BigEndian, &dataLen); err != nil {
        return nil, err
    }

    // read the data
    data := make([]byte, dataLen)
    if _, err := io.ReadFull(r, data); err != nil {
        return nil, err
    }

    // initialize the header reader
    headerReader := bytes.NewReader(headerBuf)

    // okay, read all the fields from the header buf and return the msg
    var protocolVersion uint16

    // addressing fields
    var srcEntityId uint32
    var srcChannelId uint16
    var dstEntityId uint32
    var dstChannelId uint16
    var ctrls uint16

    if err := binary.Read(headerReader, binary.BigEndian, &protocolVersion); err != nil {
        return nil, err
    }
    if err := binary.Read(headerReader, binary.BigEndian, &srcEntityId); err != nil {
        return nil, err
    }
    if err := binary.Read(headerReader, binary.BigEndian, &srcChannelId); err != nil {
        return nil, err
    }
    if err := binary.Read(headerReader, binary.BigEndian, &dstEntityId); err != nil {
        return nil, err
    }
    if err := binary.Read(headerReader, binary.BigEndian, &dstChannelId); err != nil {
        return nil, err
    }
    if err := binary.Read(headerReader, binary.BigEndian, &ctrls); err != nil {
        return nil, err
    }

    return &Packet { protocolVersion, srcEntityId, srcChannelId, dstEntityId, dstChannelId, ctrls, data }, nil
}

func WritePacket(w io.Writer, m *Packet) error {
    // TODO: make better errors
    if err := binary.Write(w, binary.BigEndian, &m.protocolVersion); err != nil {
        return err
    }
    if err := binary.Write(w, binary.BigEndian, &m.srcEntityId); err != nil {
        return err
    }
    if err := binary.Write(w, binary.BigEndian, &m.srcChannelId); err != nil {
        return err
    }
    if err := binary.Write(w, binary.BigEndian, &m.dstEntityId); err != nil {
        return err
    }
    if err := binary.Write(w, binary.BigEndian, &m.dstChannelId); err != nil {
        return err
    }
    if err := binary.Write(w, binary.BigEndian, &m.ctrls); err != nil {
        return err
    }

    // write the data values
    dataLength := uint16(len(m.data))
    if err := binary.Write(w, binary.BigEndian, &dataLength); err != nil {
        return err
    }
    if _, err := w.Write(m.data); err != nil {
        return err
    }

    return nil
}

// this is NOT thread safe!
type PacketReader struct {
    in <-chan Packet

    // current read buffer
    buf []byte
}

func NewPacketReader(in <-chan Packet) *PacketReader {
    return &PacketReader{in, nil}
}

func (self *PacketReader) Read(p []byte) (int, error) {
    if self.buf == nil {
        self.buf = (<-self.in).data
    }

    // take a local snapshot of the buffer
    buf := self.buf

    // copy as much of the buffer as possible
    n := copy(p, buf)

    // if we consumed the entire buffer, then we're done with this packet.
    if n >= len(buf) {
        self.buf = nil
        return n, nil
    }

    // move the position
    self.buf = buf[n:]
    return n, nil
}

type PacketWriter struct {
    out chan<- Packet

    srcEntityId uint32
    srcChannelId uint16

    dstEntityId uint32
    dstChannelId uint16

    maxDataLen int
}

func NewPacketWriter(out chan<- Packet, srcEntityId uint32, srcChannelId uint16, dstEntityId uint32, dstChannelId uint16) *PacketWriter {
    return &PacketWriter{out, srcEntityId, srcChannelId, dstEntityId, dstChannelId, int(PACKET_MAX_DATA_LEN)}
}

func (self *PacketWriter) Write(data []byte) (int, error) {

    // Because our packet structure defines an upper limit on the length
    // of data, we need to potentially split the data up into several
    // packets and send them individually
    //
    // To make this simple, just create a sliding window over the data
    min := 0
    max := len(data)
    end := max

    for {
        // reset max to fit within our packet limits
        if max - min > self.maxDataLen {
            max = min + self.maxDataLen - 1
        }

        // go ahead and send the packet.
        self.out<- Packet{ PROTOCOL_VERSION, self.srcEntityId, self.srcChannelId, self.dstEntityId, self.dstChannelId, 0,  data[min:max]}

        // if we reached the end of the data, we're done
        if max >= end {
            break
        }

        // move the window
        min = max
        max = end
    }

    return end, nil
}
