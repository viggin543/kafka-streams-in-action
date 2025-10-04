package pipelining

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type RecordProcessor interface {
	ProcessRecords(records []*sarama.ConsumerMessage)
	GetOffsets() map[string]map[int32]int64
}

type PipeliningConsumerClient struct {
	consumer        sarama.ConsumerGroup
	recordProcessor RecordProcessor
	topicNames      []string
	keepConsuming   bool
	mu              sync.Mutex
}

func NewPipeliningConsumerClient(brokers []string, groupID string, topicNames string, recordProcessor RecordProcessor) (*PipeliningConsumerClient, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.MaxProcessingTime = 5 * time.Second

	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	topics := strings.Split(topicNames, ",")
	return &PipeliningConsumerClient{
		consumer:        consumer,
		recordProcessor: recordProcessor,
		topicNames:      topics,
		keepConsuming:   true,
	}, nil
}

func (p *PipeliningConsumerClient) RunConsumer() {
	log.Printf("Starting runConsumer method")

	handler := &consumerGroupHandler{
		client: p,
	}

	for {
		p.mu.Lock()
		keepConsuming := p.keepConsuming
		p.mu.Unlock()

		if !keepConsuming {
			break
		}

		err := p.consumer.Consume(nil, p.topicNames, handler)
		if err != nil {
			log.Printf("Error from consumer: %v", err)
		}
	}

	log.Println("All done consuming records now")
	p.consumer.Close()
}

func (p *PipeliningConsumerClient) Close() {
	log.Println("Received signal to close")
	p.mu.Lock()
	p.keepConsuming = false
	p.mu.Unlock()
}

type consumerGroupHandler struct {
	client *PipeliningConsumerClient
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message != nil {
				messages := []*sarama.ConsumerMessage{message}
				log.Printf("Passing %d records to the processor", len(messages))
				h.client.recordProcessor.ProcessRecords(messages)

				offsetsAndMetadata := h.client.recordProcessor.GetOffsets()
				if offsetsAndMetadata != nil {
					log.Printf("Batch completed now committing the offsets %v", offsetsAndMetadata)
					for topic, partitions := range offsetsAndMetadata {
						for partition, offset := range partitions {
							session.MarkOffset(topic, partition, offset, "")
						}
					}
				} else {
					log.Println("Nothing to commit at this point")
				}
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
