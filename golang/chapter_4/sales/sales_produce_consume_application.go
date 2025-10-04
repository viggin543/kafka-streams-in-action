package sales

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const TopicName = "first-produce-consume-example"

func RunApplication() {
	log.Println("Starting the produce consume example")

	producer, err := NewSalesProducerClient(
		[]string{"localhost:9092"},
		TopicName,
		NewSalesDataSource(),
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	consumer, err := NewSalesConsumerClient(
		[]string{"localhost:9092"},
		"product-transaction-group",
		TopicName,
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	log.Println("Getting ready to start sales processing application, hit CNTL+C to stop")

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
