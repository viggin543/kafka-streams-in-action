package main

import (
	"encoding/binary"
	"fmt"
	"log"

	"example.com/kafka-avro-go/avro"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	bootstrapServers = "localhost:9092"
	groupID          = "go-avro-consumer-1"
	topic            = "avro-avengers"
)

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
			// client automatically handles transient errors; only log
			log.Printf("read error: %v", err)
			continue
		}

		if len(msg.Value) < 5 || msg.Value[0] != 0 {
			log.Printf("unexpected payload (len=%d)", len(msg.Value))
			continue
		}

		// Confluent header
		schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
		payload := msg.Value[5:]

		var rec avro.AvengerAvro
		if err := rec.UnmarshalJSON(payload); err != nil {
			log.Printf("avro decode error: %v", err)
			continue
		}

		fmt.Printf("📥 key=%s schemaID=%d => %#v\n", string(msg.Key), schemaID, rec)
	}
}
