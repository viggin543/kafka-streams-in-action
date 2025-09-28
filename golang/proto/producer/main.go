package main

import (
	"context"
	"encoding/binary"
	pb "example.com/kafka-avro-go/proto/example.com/kafka-go"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/riferrei/srclient"
	"google.golang.org/protobuf/proto"
)

const (
	bootstrapServers = "localhost:9092"
	topic            = "grpc-avengers"

	schemaRegistryURL = "http://localhost:8081"
	subject           = "grpc-avengers-value"
)

func main() {

	sr := srclient.NewSchemaRegistryClient(schemaRegistryURL)
	schema, err := sr.GetLatestSchema(subject)
	if err != nil {
		log.Fatalf("schema lookup failed: %v", err)
	}
	schemaId := schema.ID()
	log.Printf("Using schema id: %d", schemaId)

	msgPB := &pb.AvengerProto{
		Name:     "Captain America",
		RealName: "Steve Rogers",
		Movies:   []string{"The First Avenger", "The Winter Soldier", "Civil War"},
	}
	value := marshalEvent(err, msgPB, schemaId)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	go func() {
		for eventsChan := range producer.Events() {
			switch ev := eventsChan.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("❌ delivery failed: %v", ev.TopicPartition.Error)
				} else {
					log.Printf("✅ delivered to %v", ev.TopicPartition)
				}
			}
		}
	}()

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: getTopic(), Partition: kafka.PartitionAny},
		Key:            []byte("cap"),
		Value:          value,
	}, nil)
	if err != nil {
		log.Fatalf("produce error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	producer.Flush(10_000)

	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
	}

	fmt.Println("Producer done.")
}

func getTopic() *string {
	t := topic
	s := &t
	return s
}

func marshalEvent(err error, msgPB *pb.AvengerProto, schemaId int) []byte {
	payload, err := proto.Marshal(msgPB)
	if err != nil {
		log.Fatalf("proto marshal: %v", err)
	}

	// Confluent Protobuf wire header: magic(0), schemaID(4), messageIndex(varint=0)
	value := make([]byte, 0, 5+1+len(payload))
	value = append(value, 0x00)
	idbuf := make([]byte, 4)
	binary.BigEndian.PutUint32(idbuf, uint32(schemaId))
	value = append(value, idbuf...)
	value = append(value, 0x00) // message index = 0 (first message in the .proto)
	value = append(value, payload...)
	return value
}
