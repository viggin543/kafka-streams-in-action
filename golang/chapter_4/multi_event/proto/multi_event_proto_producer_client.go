package proto

import (
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type Event struct {
	Key   string
	Value []byte
}

type EventDataSource interface {
	Fetch() []Event
}

type MultiEventProtoProducerClient struct {
	producer      sarama.SyncProducer
	topicName     string
	dataSource    EventDataSource
	keepProducing bool
	runOnce       bool
	mu            sync.Mutex
}

func NewMultiEventProtoProducerClient(brokers []string, topicName string, dataSource EventDataSource) (*MultiEventProtoProducerClient, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &MultiEventProtoProducerClient{
		producer:      producer,
		topicName:     topicName,
		dataSource:    dataSource,
		keepProducing: true,
		runOnce:       false,
	}, nil
}

func (m *MultiEventProtoProducerClient) RunProducer() {
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
			msg := &sarama.ProducerMessage{
				Topic: m.topicName,
				Key:   sarama.StringEncoder(event.Key),
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

func (m *MultiEventProtoProducerClient) RunProducerOnce() {
	m.mu.Lock()
	m.runOnce = true
	m.mu.Unlock()
	m.RunProducer()
}

func (m *MultiEventProtoProducerClient) Close() {
	log.Println("Received signal to close")
	m.mu.Lock()
	m.keepProducing = false
	m.mu.Unlock()
}
