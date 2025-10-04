package pipelining

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

const TopicName = "no-auto-commit-application"

func RunApplication() {
	log.Println("Starting the produce consume example")

	offsetQueue := make(chan map[string]map[int32]int64, 25)
	productQueue := make(chan []*sarama.ConsumerMessage, 25)

	recordProcessor := NewConcurrentRecordProcessor(offsetQueue, productQueue)

	pipeliningProducerClient, err := NewPipeliningProducerClient([]string{"localhost:9092"}, TopicName)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	pipeliningConsumerClient, err := NewPipeliningConsumerClient(
		[]string{"localhost:9092"},
		"product-non-auto-commit-group",
		TopicName,
		recordProcessor,
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	log.Println("Getting ready to start concurrent no auto-commit processing application, hit CNTL+C to stop")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		pipeliningProducerClient.RunProducer()
	}()
	log.Println("Started producer thread")

	wg.Add(1)
	go func() {
		defer wg.Done()
		pipeliningConsumerClient.RunConsumer()
	}()
	log.Println("Started consumer thread")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm
	log.Println("Starting shutdown")
	pipeliningProducerClient.Close()
	pipeliningConsumerClient.Close()
	recordProcessor.Close()

	wg.Wait()
	log.Println("All done now")
}
