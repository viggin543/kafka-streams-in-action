package pipelining

import (
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type OffsetInfo struct {
	Topic     string
	Partition int32
	Offset    int64
}

type ConcurrentRecordProcessor struct {
	offsetQueue  chan map[string]map[int32]int64
	productQueue chan []*sarama.ConsumerMessage
	keepProcessing bool
	mu           sync.Mutex
	wg           sync.WaitGroup
}

func NewConcurrentRecordProcessor(offsetQueue chan map[string]map[int32]int64, productQueue chan []*sarama.ConsumerMessage) *ConcurrentRecordProcessor {
	processor := &ConcurrentRecordProcessor{
		offsetQueue:    offsetQueue,
		productQueue:   productQueue,
		keepProcessing: true,
	}
	processor.wg.Add(1)
	go processor.process()
	return processor
}

func (c *ConcurrentRecordProcessor) ProcessRecords(records []*sarama.ConsumerMessage) {
	select {
	case c.productQueue <- records:
		log.Printf("Putting %d records into the process queue", len(records))
	case <-time.After(20 * time.Second):
		log.Println("Timeout putting records into queue")
	}
}

func (c *ConcurrentRecordProcessor) GetOffsets() map[string]map[int32]int64 {
	select {
	case offsets := <-c.offsetQueue:
		return offsets
	default:
		return nil
	}
}

func (c *ConcurrentRecordProcessor) process() {
	defer c.wg.Done()

	for {
		c.mu.Lock()
		keepProcessing := c.keepProcessing
		c.mu.Unlock()

		if !keepProcessing {
			break
		}

		select {
		case consumerRecords := <-c.productQueue:
			if consumerRecords != nil {
				offsets := make(map[string]map[int32]int64)

				partitionMap := make(map[string]map[int32][]*sarama.ConsumerMessage)
				for _, record := range consumerRecords {
					if partitionMap[record.Topic] == nil {
						partitionMap[record.Topic] = make(map[int32][]*sarama.ConsumerMessage)
					}
					partitionMap[record.Topic][record.Partition] = append(partitionMap[record.Topic][record.Partition], record)
				}

				for topic, partitions := range partitionMap {
					if offsets[topic] == nil {
						offsets[topic] = make(map[int32]int64)
					}
					for partition, records := range partitions {
						for _, record := range records {
							c.doProcessRecord(record)
						}
						lastOffset := records[len(records)-1].Offset
						offsets[topic][partition] = lastOffset + 1
					}
				}

				log.Printf("putting offsets and metadata %v in queue", offsets)
				c.offsetQueue <- offsets
			} else {
				log.Println("No records in the product queue at the moment")
			}
		case <-time.After(20 * time.Second):
			log.Println("No records in the product queue at the moment")
		}
	}
}

func (c *ConcurrentRecordProcessor) doProcessRecord(record *sarama.ConsumerMessage) {
	log.Printf("Processing order for key %s at offset %d", string(record.Key), record.Offset)
	time.Sleep(1 * time.Second)
}

func (c *ConcurrentRecordProcessor) Close() {
	c.mu.Lock()
	c.keepProcessing = false
	c.mu.Unlock()
	c.wg.Wait()
}
