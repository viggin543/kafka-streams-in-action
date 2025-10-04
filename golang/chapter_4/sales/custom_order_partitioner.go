package sales

import (
	"hash/fnv"

	"github.com/IBM/sarama"
)

type CustomOrderPartitioner struct{}

func NewCustomOrderPartitioner(topic string) sarama.Partitioner {
	return &CustomOrderPartitioner{}
}

func (p *CustomOrderPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	key, err := message.Key.Encode()
	if err != nil {
		return 0, err
	}

	strKey := string(key)
	if strKey == "CUSTOM" {
		return 0, nil
	}

	h := fnv.New32a()
	h.Write(key)
	hash := h.Sum32()

	partition := int32(hash % uint32(numPartitions))
	return partition, nil
}

func (p *CustomOrderPartitioner) RequiresConsistency() bool {
	return true
}
