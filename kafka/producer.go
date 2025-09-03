package kafka

import (
	"context"
	"log"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(broker string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	log.Printf("Kafka producer created for topic: %s", topic)
	
	return &Producer{writer: writer}
}

func (p *Producer) SendMessage(ctx context.Context, key, value string) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	})
	
	if err != nil {
		log.Printf("Failed to send message to Kafka: %v", err)
		return err
	}
	
	log.Printf("Message sent to Kafka - Key: %s, Value: %s", key, value)
	return nil
}

func (p *Producer) Close() error {
	err := p.writer.Close()
	if err != nil {
		log.Printf("Failed to close Kafka producer: %v", err)
		return err
	}
	log.Println("Kafka producer closed successfully")
	return nil
}