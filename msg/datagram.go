package msg

import "io"
import "bytes"
import "encoding/binary"
//import "errors"

const protocolVersion = 1

// A multiplex datagram is the base message that is
// routed amongst multiplex channels.
//
// As constructed, this assumes the following of the underlying stream:
//
//   -> reliable, in-order delivery (e.g. TCP)
//
// TODO: Add support for fragmentation.  We don't want to swamp a connection
// TODO: with a single datagram.  As it's likely that many simultaneous
// TODO: conversations will be occurring.
type Datagram struct {

    // necessary for backwards/forwards compatibility
    protocolVersion uint16

    // every datagram must identify its source
    srcEntityId uint32
    srcChannelId uint16

    // every datagram must identify its destination
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
func readDatagram(r io.Reader) (*Datagram, error) {
    // read all the header bytes
    headerBuf := make([]byte, 16)
    if _, err := io.ReadFull(r, headerBuf); err != nil {
        return nil, err
    }

    // read the data length bytes
    dataLenBuf := make([]byte, 4)
    if _, err := io.ReadFull(r, dataLenBuf); err != nil {
        return nil, err
    }

    // parse the length
    var dataLen uint32
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

    // session fields
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
    if err := binary.Read(headerReader, binary.BigEndian, &dstChannelId); err != nil {
        return nil, err
    }
    if err := binary.Read(headerReader, binary.BigEndian, &ctrls); err != nil {
        return nil, err
    }

    return &Datagram { protocolVersion, srcEntityId, srcChannelId, dstEntityId, dstChannelId, ctrls, data }, nil
}

func (m *Datagram) write(w io.Writer) error {
    // write the header values

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
    dataLength := uint32(len(m.data))
    if err := binary.Write(w, binary.BigEndian, &dataLength); err != nil {
        return err
    }
    if _, err := w.Write(m.data); err != nil {
        return err
    }

    return nil
}
