package msg

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"unsafe"
	"log"
)

//Msg struct
type Msg struct {
	metadata Metadata
	data     []byte
}

//Metadata struct
//Represent the metadata of a message
type Metadata struct {
	ClientID  uint32
	MsgId     uint32
	Timestamp int64
}

//Stat
//stat for messages
type Statistics struct {
	ReceivedMsg     int32
	CumulativeDelay int64
}

//the function parses the metadata from a []byte message and returns
//a Metadata struct
func ParseMetadata(msg []byte) Metadata {
	metadata := Metadata{ClientID: binary.BigEndian.Uint32(msg[:4]),
		MsgId:     binary.BigEndian.Uint32(msg[4:8]),
		Timestamp: int64(binary.BigEndian.Uint64(msg[8:16])),
	}
	return metadata
}

//returns the serialized version of Msg
func (m *Msg) Bytes() []byte {
	//log.Print(m)
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, m.metadata) //TODO to be tested, is this writing Msg or pointer?
	if err != nil {
		log.Print(err)
	}
	return append(buf.Bytes(), m.data...)
}

//creates a new []byte representing the message
func New(clientId uint32, msgId uint32, timestamp int64, msgLen int) (*Msg, error) {

	metadata := Metadata{
		ClientID:  clientId,
		MsgId:     msgId,
		Timestamp: timestamp,
	}

	metadataSize := int(unsafe.Sizeof(&metadata))
	if msgLen <= metadataSize { //if desired msg length is <= metadata size, metadata won't fit
		return nil, errors.New("The selected msg length is too short")
	}
	data := make([]byte, msgLen-metadataSize)
	io.ReadFull(rand.Reader, data)

	return &Msg{metadata: Metadata{
		ClientID:  clientId,
		MsgId:     msgId,
		Timestamp: timestamp,
	},
		data: data}, nil
}
