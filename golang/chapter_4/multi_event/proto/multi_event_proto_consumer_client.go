package proto

import (
	"context"
	"log"
	"strings"
	"sync"

	"github.com/IBM/sarama"
)

type MultiEventProtoConsumerClient struct {
	consumer      sarama.ConsumerGroup
	topicNames    []string
	keepConsuming bool
	runOnce       bool
	mu            sync.Mutex
	purchases     [][]byte
	logins        [][]byte
	searches      [][]byte
	eventsList    [][]byte
}

func NewMultiEventProtoConsumerClient(brokers []string, groupID string, topicNames string) (*MultiEventProtoConsumerClient, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	topics := strings.Split(topicNames, ",")
	return &MultiEventProtoConsumerClient{
		consumer:      consumer,
		topicNames:    topics,
		keepConsuming: true,
		runOnce:       false,
		purchases:     make([][]byte, 0),
		logins:        make([][]byte, 0),
		searches:      make([][]byte, 0),
		eventsList:    make([][]byte, 0),
	}, nil
}

func (m *MultiEventProtoConsumerClient) RunConsumer() {
	log.Printf("Starting runConsumer method")

	handler := &protoConsumerGroupHandler{
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
		err := m.consumer.Consume(context.Background(), m.topicNames, handler)
		if err != nil {
			log.Printf("Error from consumer: %v", err)
		}
	}

	log.Println("All done consuming records now")
	m.consumer.Close()
}

func (m *MultiEventProtoConsumerClient) RunConsumerOnce() {
	m.mu.Lock()
	m.runOnce = true
	m.mu.Unlock()
	m.RunConsumer()
}

func (m *MultiEventProtoConsumerClient) Close() {
	log.Println("Received signal to close")
	m.mu.Lock()
	m.keepConsuming = false
	m.mu.Unlock()
}

type protoConsumerGroupHandler struct {
	client *MultiEventProtoConsumerClient
}

func (h *protoConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *protoConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *protoConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message != nil {
				eventType := h.getEventType(message.Value)
				log.Printf("Found event %s for user %s", eventType, string(message.Key))
				session.MarkMessage(message, "")

				h.client.mu.Lock()
				runOnce := h.client.runOnce
				h.client.mu.Unlock()

				if runOnce {
					h.client.Close()
				}
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *protoConsumerGroupHandler) getEventType(value []byte) string {
	h.client.eventsList = append(h.client.eventsList, value)
	return "event"
}
