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

	salesDataSource := NewSalesDataSourceDefault()

	salesProducerClient, err := NewSalesProducerClient(
		[]string{"localhost:9092"},
		TopicName,
		salesDataSource,
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	salesConsumerClient, err := NewSalesConsumerClient(
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
		salesProducerClient.RunProducer()
	}()
	log.Println("Started producer thread")

	wg.Add(1)
	go func() {
		defer wg.Done()
		salesConsumerClient.RunConsumer()
	}()
	log.Println("Started consumer thread")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm
	log.Println("Starting shutdown")
	salesProducerClient.Close()
	salesConsumerClient.Close()

	wg.Wait()
	log.Println("All done now")
}
