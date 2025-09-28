package main

import (
	pb "example.com/kafka-avro-go/proto/example.com/kafka-go"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	pbserde "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"os"
	"os/signal"
)

const topic = "grpc-avengers"

// no low-level byte manipulation when serializing the message
// using confluent dependencies

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "g1",
		"auto.offset.reset": "earliest",
	})
	must(err)
	defer c.Close()
	must(c.SubscribeTopics([]string{topic}, nil))
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig("http://localhost:8081"))
	must(err)
	des, err := pbserde.NewDeserializer(srClient, serde.ValueSerde, pbserde.NewDeserializerConfig())
	must(err)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	for {
		select {
		case <-stop:
			fmt.Println("bye")
			return
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch m := ev.(type) {
			case *kafka.Message:
				var out pb.AvengerProto
				err := des.DeserializeInto(*m.TopicPartition.Topic, m.Value, &out)
				if err != nil {
					fmt.Printf("deserialize error: %v\n", err)
					continue
				}
				fmt.Printf("got Avenger: name=%s real=%s movies=%v\n", out.Name, out.RealName, out.Movies)
			case kafka.Error:
				fmt.Printf("kafka error: %v\n", m)
			}
		}
	}
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
