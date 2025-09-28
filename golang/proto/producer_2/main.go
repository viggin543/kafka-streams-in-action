package main

import (
	pb "example.com/kafka-avro-go/proto/example.com/kafka-go"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	pbserde "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
)

func main() {
	const (
		bootstrap = "localhost:9092"
		registry  = "http://localhost:8081"
		topic     = "grpc-avengers"
	)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
	if err != nil {
		panic(err)
	}
	defer producer.Close()
	sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(registry))
	if err != nil {
		panic(err)
	}
	ser, err := pbserde.NewSerializer(sr, serde.ValueSerde, pbserde.NewSerializerConfig())
	if err != nil {
		panic(err)
	}
	msg := &pb.AvengerProto{
		Name:     "Hulk",
		RealName: "Bruce Banner",
		Movies:   []string{"The Avengers", "Age of Ultron"},
	}
	val, err := ser.Serialize(topic, msg)
	if err != nil {
		panic(err)
	}
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &[]string{topic}[0], Partition: kafka.PartitionAny},
		Value:          val,
		Key:            []byte("hulk"),
	}, nil)
	if err != nil {
		panic(err)
	}
	producer.Flush(5000)
	fmt.Println("✅ Produced Hulk")
}
