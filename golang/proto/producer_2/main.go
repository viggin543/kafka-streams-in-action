package main

import (
	"fmt"

	pb "example.com/kafka-avro-go/proto/example.com/kafka-go"
	"example.com/kafka-avro-go/util"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	pbserde "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
)

// high level serialization
func main() {
	const (
		bootstrap = "localhost:9092"
		registry  = "http://localhost:8081"
		topic     = "grpc-avengers"
	)

	producer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
	defer producer.Close()
	sr, _ := schemaregistry.NewClient(schemaregistry.NewConfig(registry))
	ser, _ := pbserde.NewSerializer(sr, serde.ValueSerde, pbserde.NewSerializerConfig())
	msg := &pb.AvengerProto{
		Name:     "Hulk",
		RealName: "Bruce Banner",
		Movies:   []string{"The Avengers", "Age of Ultron"},
	}
	val, _ := ser.Serialize(topic, msg)
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: asPtr(topic), Partition: kafka.PartitionAny},
		Value:          val,
		Key:            []byte("hulk"),
	}, nil)
	util.PanicOnErr(err)
	producer.Flush(5000)
	fmt.Println("✅ Produced Hulk")
}

func asPtr(topic string) *string {
	return &[]string{topic}[0]
}
