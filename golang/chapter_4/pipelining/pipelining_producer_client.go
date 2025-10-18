package pipelining

import (
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type ProducerClient struct {
	producer      sarama.SyncProducer
	topicName     string
	keepProducing bool
	mu            sync.Mutex
}

func NewPipeliningProducerClient(brokers []string, topicName string) (*ProducerClient, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &ProducerClient{
		producer:      producer,
		topicName:     topicName,
		keepProducing: true,
	}, nil
}

func (p *ProducerClient) RunProducer() {
	log.Printf("Created producer instance")

	for {
		p.mu.Lock()
		keepProducing := p.keepProducing
		p.mu.Unlock()

		if !keepProducing {
			break
		}

		purchases := generateProductTransactions(25)
		log.Printf("Received %d sales data records", len(purchases))

		for _, purchase := range purchases {
			msg := &sarama.ProducerMessage{
				Topic: p.topicName,
				Key:   sarama.StringEncoder(purchase.CustomerName),
				Value: sarama.ByteEncoder(purchase.Value),
			}

			_, _, err := p.producer.SendMessage(msg)
			if err != nil {
				log.Printf("Error producing records: %v", err)
			}
		}

		time.Sleep(3 * time.Second)
	}

	log.Println("Producer loop exiting now")
	p.producer.Close()
}

func (p *ProducerClient) Close() {
	log.Println("Received signal to close")
	p.mu.Lock()
	p.keepProducing = false
	p.mu.Unlock()
}

type ProductTransaction struct {
	CustomerName string
	ProductName  string
	Quantity     int
	Price        float64
	Value        []byte
}

func generateProductTransactions(count int) []ProductTransaction {
	transactions := make([]ProductTransaction, count)
	for i := 0; i < count; i++ {
		transactions[i] = ProductTransaction{
			CustomerName: "Customer",
			ProductName:  "Product",
			Quantity:     1,
			Price:        10.0,
			Value:        []byte("transaction"),
		}
	}
	return transactions
}
