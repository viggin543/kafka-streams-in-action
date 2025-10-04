package main

import (
	"fmt"

	pb "example.com/kafka-avro-go/proto/example.com/kafka-go"
	"example.com/kafka-avro-go/util"
	"github.com/IBM/sarama"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	pbserde "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/riferrei/srclient"
)

var protoSchema = `syntax = "proto3";
package example.com.kafka_go;

message AvengerProto {
  string name = 1;
  string real_name = 2;
  repeated string movies = 3;
}`

const (
	bootstrap = "localhost:9092"
	registry  = "http://localhost:8081"
	topic     = "grpc-avengers"
)

func main() {
	_ = assertSchema(topic)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, _ := sarama.NewSyncProducer([]string{bootstrap}, config)
	defer producer.Close()

	sr, _ := schemaregistry.NewClient(schemaregistry.NewConfig(registry))
	ser, _ := pbserde.NewSerializer(sr, serde.ValueSerde, pbserde.NewSerializerConfig())
	msg := &pb.AvengerProto{
		Name:     "Hulk",
		RealName: "Bruce banana",
		Movies:   []string{"The Avengers", "banana"},
	}
	val, _ := ser.Serialize(topic, msg)

	kafkaMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("hulk"),
		Value: sarama.ByteEncoder(val),
	}

	partition, offset, err := producer.SendMessage(kafkaMsg)
	util.PanicOnErr(err)

	fmt.Printf("✅ Produced Hulk to partition %d at offset %d\n", partition, offset)
}

func assertSchema(topic string) error {
	srClient := srclient.NewSchemaRegistryClient(registry)

	_, err := srClient.GetLatestSchema(topic + "-value")
	if err != nil {
		_, err = srClient.CreateSchema(topic+"-value", protoSchema, srclient.Protobuf)
		util.PanicOnErr(err)
	}
	return err
}
