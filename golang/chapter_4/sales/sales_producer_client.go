package sales

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type SalesProducerClient struct {
	producer       sarama.SyncProducer
	topicName      string
	salesDataSource *SalesDataSource
	keepProducing  bool
	mu             sync.Mutex
}

func NewSalesProducerClient(brokers []string, topicName string, salesDataSource *SalesDataSource) (*SalesProducerClient, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = NewCustomOrderPartitioner

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &SalesProducerClient{
		producer:        producer,
		topicName:       topicName,
		salesDataSource: salesDataSource,
		keepProducing:   true,
	}, nil
}

func (s *SalesProducerClient) RunProducer() {
	log.Printf("Created producer instance")

	for {
		s.mu.Lock()
		keepProducing := s.keepProducing
		s.mu.Unlock()

		if !keepProducing {
			break
		}

		purchases := s.salesDataSource.Fetch()
		log.Printf("Received sales data %v", purchases)

		for _, purchase := range purchases {
			value, _ := json.Marshal(purchase)
			msg := &sarama.ProducerMessage{
				Topic: s.topicName,
				Key:   sarama.StringEncoder(purchase.CustomerName),
				Value: sarama.ByteEncoder(value),
			}

			partition, offset, err := s.producer.SendMessage(msg)
			if err != nil {
				log.Printf("Error producing records: %v", err)
			} else {
				log.Printf("Produced record at offset %d with timestamp on partition %d", offset, partition)
			}
		}

		time.Sleep(3 * time.Second)
	}

	log.Println("Producer loop exiting now")
	s.producer.Close()
}

func (s *SalesProducerClient) Close() {
	log.Println("Received signal to close")
	s.mu.Lock()
	s.keepProducing = false
	s.mu.Unlock()
}
