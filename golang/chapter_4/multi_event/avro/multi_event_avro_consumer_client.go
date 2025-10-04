package avro

import (
	"log"
	"strings"
	"sync"

	"github.com/IBM/sarama"
)

type MultiEventAvroConsumerClient struct {
	consumer        sarama.ConsumerGroup
	topicNames      []string
	keepConsuming   bool
	runOnce         bool
	mu              sync.Mutex
	consumedRecords [][]byte
}

func NewMultiEventAvroConsumerClient(brokers []string, groupID string, topicNames string) (*MultiEventAvroConsumerClient, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	topics := strings.Split(topicNames, ",")
	return &MultiEventAvroConsumerClient{
		consumer:        consumer,
		topicNames:      topics,
		keepConsuming:   true,
		runOnce:         false,
		consumedRecords: make([][]byte, 0),
	}, nil
}

func (m *MultiEventAvroConsumerClient) RunConsumer() {
	log.Printf("Starting runConsumer method")

	handler := &avroConsumerGroupHandler{
		client: m,
	}

	for {
		m.mu.Lock()
		keepConsuming := m.keepConsuming
		m.mu.Unlock()

		if !keepConsuming {
			break
		}

		log.Printf("Subscribing to %v", m.topicNames)
		err := m.consumer.Consume(nil, m.topicNames, handler)
		if err != nil {
			log.Printf("Error from consumer: %v", err)
		}
	}

	log.Println("All done consuming records now")
	m.consumer.Close()
}

func (m *MultiEventAvroConsumerClient) RunConsumerOnce() {
	m.mu.Lock()
	m.runOnce = true
	m.mu.Unlock()
	m.RunConsumer()
}

func (m *MultiEventAvroConsumerClient) Close() {
	log.Println("Received signal to close")
	m.mu.Lock()
	m.keepConsuming = false
	m.mu.Unlock()
}

type avroConsumerGroupHandler struct {
	client *MultiEventAvroConsumerClient
}

func (h *avroConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *avroConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *avroConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message != nil {
				h.client.mu.Lock()
				h.client.consumedRecords = append(h.client.consumedRecords, message.Value)
				runOnce := h.client.runOnce
				h.client.mu.Unlock()

				log.Printf("Found an Avro event")

				session.MarkMessage(message, "")

				if runOnce {
					h.client.Close()
				}
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
