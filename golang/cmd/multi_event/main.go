package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"example.com/kafka-avro-go/chapter_4/multi_event/proto"
)

type source struct{}

func (s *source) Fetch() []proto.Event {
	return []proto.Event{
		{Key: "foo", Value: []byte("{}")},
		{Key: "bar", Value: []byte(`{"name": "bar"}`)},
	}
}

func main() {

	producer, err := proto.NewMultiEventProtoProducerClient([]string{"localhost:9092"},
		"multi-event-topic",
		&source{},
	)
	if err != nil {
		panic(err)
	}
	consumer, err := proto.NewMultiEventProtoConsumerClient([]string{"localhost:9092"}, "fii", "multi-event-topic")
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		producer.RunProducer()
	}()
	log.Println("Started producer thread")

	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer.RunConsumer()
	}()
	log.Println("Started consumer thread")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm
	log.Println("Starting shutdown")
	producer.Close()
	consumer.Close()

	wg.Wait()
	log.Println("All done now")
}
