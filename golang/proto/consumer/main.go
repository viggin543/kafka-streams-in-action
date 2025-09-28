package main

import (
	"encoding/binary"
	pb "example.com/kafka-avro-go/proto/example.com/kafka-go"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

const (
	bootstrapServers = "localhost:9092"
	groupID          = "go-proto-consumer-1"
	topic            = "grpc-avengers"
)

// low-level byte manipulation when serializing the message
// less dependencies

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		log.Fatal(err)
	}

	log.Printf("🚀 consuming from %s ...", topic)

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			log.Printf("read error: %v", err)
			continue
		}

		val := msg.Value
		if len(val) < 6 || val[0] != 0x00 {
			log.Printf("unexpected payload (len=%d)", len(val))
			continue
		}

		schemaID := binary.BigEndian.Uint32(val[1:5])

		idx, n := readUVarint(val[5:])
		// idx is the proto message idx in the proto file.
		// since  our case its always 0
		// since proto file has only one message
		if n <= 0 {
			log.Printf("invalid varint for message index")
			continue
		}
		payload := val[5+n:]

		var rec pb.AvengerProto
		if err := proto.Unmarshal(payload, &rec); err != nil {
			log.Printf("protobuf decode error: %v", err)
			continue
		}

		fmt.Printf("📥 key=%s schemaID=%d msgIndex=%d => %#v\n", string(msg.Key), schemaID, idx, &rec)
	}
}

// minimal unsigned varint reader
func readUVarint(buf []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range buf {
		if b < 0x80 {
			if i > 9 || i == 9 && b > 1 {
				return 0, -1 // overflow
			}
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0 // buffer too small
}
