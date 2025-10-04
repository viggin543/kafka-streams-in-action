package avro

import (
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type AvroRecord struct {
	Key   string
	Value []byte
}

type AvroDataSource interface {
	Fetch() []AvroRecord
}

type MultiEventAvroProducerClient struct {
	producer      sarama.SyncProducer
	topicName     string
	keyField      string
	dataSource    AvroDataSource
	keepProducing bool
	runOnce       bool
	mu            sync.Mutex
}

func NewMultiEventAvroProducerClient(brokers []string, topicName string, keyField string, dataSource AvroDataSource) (*MultiEventAvroProducerClient, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	if keyField == "" {
		keyField = "id"
	}

	return &MultiEventAvroProducerClient{
		producer:      producer,
		topicName:     topicName,
		keyField:      keyField,
		dataSource:    dataSource,
		keepProducing: true,
		runOnce:       false,
	}, nil
}

func (m *MultiEventAvroProducerClient) RunProducer() {
	log.Printf("Created producer instance")

	for {
		m.mu.Lock()
		keepProducing := m.keepProducing
		runOnce := m.runOnce
		m.mu.Unlock()

		if !keepProducing {
			break
		}

		events := m.dataSource.Fetch()
		for _, event := range events {
			key := m.extractKey(event)
			msg := &sarama.ProducerMessage{
				Topic: m.topicName,
				Key:   sarama.StringEncoder(key),
				Value: sarama.ByteEncoder(event.Value),
			}

			_, _, err := m.producer.SendMessage(msg)
			if err != nil {
				log.Printf("Error producing records: %v", err)
			}
		}

		if runOnce {
			m.Close()
		}
	}

	log.Println("Producer loop exiting now")
	m.producer.Close()
}

func (m *MultiEventAvroProducerClient) extractKey(record AvroRecord) string {
	if record.Key != "" {
		return record.Key
	}
	return ""
}

func (m *MultiEventAvroProducerClient) RunProducerOnce() {
	m.mu.Lock()
	m.runOnce = true
	m.mu.Unlock()
	m.RunProducer()
}

func (m *MultiEventAvroProducerClient) Close() {
	log.Println("Received signal to close")
	m.mu.Lock()
	m.keepProducing = false
	m.mu.Unlock()
}
