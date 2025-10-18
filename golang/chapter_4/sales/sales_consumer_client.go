package sales

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type ConsumerClient struct {
	consumer      sarama.ConsumerGroup
	topicNames    []string
	keepConsuming bool
	mu            sync.Mutex
}

func NewSalesConsumerClient(brokers []string, groupID string, topicNames string) (*ConsumerClient, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second

	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	topics := strings.Split(topicNames, ",")
	return &ConsumerClient{
		consumer:      consumer,
		topicNames:    topics,
		keepConsuming: true,
	}, nil
}

func (s *ConsumerClient) RunConsumer() {
	log.Printf("Starting runConsumer method")

	handler := &salesConsumerGroupHandler{client: s}

	for {
		s.mu.Lock()
		keepConsuming := s.keepConsuming
		s.mu.Unlock()

		if !keepConsuming {
			break
		}

		err := s.consumer.Consume(context.Background(), s.topicNames, handler)
		if err != nil {
			log.Printf("Error from consumer: %v", err)
		}
	}

	log.Println("All done consuming records now")
	s.consumer.Close()
}

func (s *ConsumerClient) Close() {
	log.Println("Received signal to close")
	s.mu.Lock()
	s.keepConsuming = false
	s.mu.Unlock()
}

type salesConsumerGroupHandler struct {
	client *ConsumerClient
}

func (h *salesConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *salesConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *salesConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message != nil {
				var pt ProductTransaction
				json.Unmarshal(message.Value, &pt)
				log.Printf("Sale for %s with product %s for a total sale of %f on partition %d",
					string(message.Key),
					pt.ProductName,
					float64(pt.Quantity)*pt.Price,
					message.Partition)
				session.MarkMessage(message, "")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
