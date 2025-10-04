package util

import (
	"encoding/binary"
	"log"

	"google.golang.org/protobuf/proto"
)

func PanicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func MarshalEvent(msgPB proto.Message, schemaId int) []byte {
	payload, err := proto.Marshal(msgPB)
	if err != nil {
		log.Fatalf("proto marshal: %v", err)
	}
	// Initialize byte slice to hold magic byte, schema ID, message index, and payload
	value := make([]byte, 0, 5+1+len(payload))
	// Add magic byte (0x00) as first byte to indicate Confluent wire format
	value = append(value, 0x00)
	// Create 4-byte buffer to hold schema ID
	idbuf := make([]byte, 4)
	// Convert schema ID to 4 bytes in big-endian order
	binary.BigEndian.PutUint32(idbuf, uint32(schemaId))
	// Add the 4-byte schema ID after magic byte
	value = append(value, idbuf...)
	// Add message index byte (0x00) to indicate first message in schema
	value = append(value, 0x00) // message index = 0 (first message in the .proto)
	// Add the serialized protocol buffer payload at the end
	value = append(value, payload...)
	return value
}
