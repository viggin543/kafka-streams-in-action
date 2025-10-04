package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	pb "example.com/kafka-avro-go/proto/example.com/kafka-go"
	"example.com/kafka-avro-go/util"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/riferrei/srclient"
)

const (
	bootstrapServers = "localhost:9092"
	topic            = "grpc-avengers"

	schemaRegistryURL = "http://localhost:8081"
	subject           = "grpc-avengers-value"
)

// low level serialization
func main() {

	sr := srclient.NewSchemaRegistryClient(schemaRegistryURL)
	schema, _ := sr.GetLatestSchema(subject)
	log.Printf("Using schema id: %d", schema.ID())

	value := util.MarshalEvent(&pb.AvengerProto{
		Name:     "Captain America",
		RealName: "Steve Rogers",
		Movies:   []string{"The First Avenger", "The Winter Soldier", "Civil War"},
	}, schema.ID())

	producer, _ := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
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

	_ = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: getTopic(), Partition: kafka.PartitionAny},
		Key:            []byte("cap"),
		Value:          value,
	}, nil)

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
