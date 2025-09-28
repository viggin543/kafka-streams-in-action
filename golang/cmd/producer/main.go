package main

import (
	"context"
	"encoding/binary"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	// generated package from gogen-avro step
	"example.com/kafka-avro-go/avro"
)

const (
	bootstrapServers = "localhost:9092"
	schemaID         = 9               // from your subject info
	topic            = "avro-avengers" // subject is avro-avengers-value
)

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func wireFormat(schemaID int, avroBytes []byte) []byte {
	header := make([]byte, 5)
	header[0] = 0
	binary.BigEndian.PutUint32(header[1:], uint32(schemaID))
	return append(header, avroBytes...)
}

func main() {
	p := must(kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	}))

	defer p.Close()

	// delivery reports in background
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("❌ delivery failed: %v", ev.TopicPartition.Error)
				} else {
					log.Printf("✅ delivered to %v", ev.TopicPartition)
				}
			}
		}
	}()

	// Build a record using the generated type
	record := &avro.AvengerAvro{
		Name:      "Captain America",
		Real_name: "Steve Rogers",
		Movies:    []string{"The First Avenger", "The Winter Soldier", "Civil War"},
	}

	// Serialize to Avro binary (no SR header yet)
	// gogen-avro generates a method that writes into an io.Writer.
	// Use the helper below:
	avroBytes := must(record.MarshalJSON())

	// Prepend Confluent wire-format header
	value := wireFormat(schemaID, avroBytes)

	// Produce
	t := topic
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &t, Partition: kafka.PartitionAny},
		Value:          value,
		// Optional key if you want partitioning
		Key: []byte("cap"),
	}

	err := p.Produce(msg, nil)
	if err != nil {
		log.Fatalf("produce error: %v", err)
	}

	// Wait for delivery
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	p.Flush(10_000) // up to 10s

	// Keep the process briefly alive to catch delivery report logs
	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
	}
}
