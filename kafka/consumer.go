package kafka

import (
	"context"
	"log"
	"simple-kafka-app/database"
	"simple-kafka-app/cache"
	"simple-kafka-app/models"

	kafkago "github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafkago.Reader
}

func NewConsumer(broker string, topic string, groupID string) *Consumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	log.Printf("Kafka consumer created for topic: %s, group: %s", topic, groupID)
	
	return &Consumer{reader: reader}
}

func (c *Consumer) ReadAndProcessOrder(ctx context.Context, db *database.DB, cache *cache.OrderCache) error {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		log.Printf("Failed to read message from Kafka: %v", err)
		return err
	}

	log.Printf("Message received from Kafka - Key: %s, Value: %s", 
		string(msg.Key), string(msg.Value))


	order, err := models.ParseOrderFromJSON(msg.Value)
	if err != nil {
		log.Printf("Failed to parse order message: %v", err)
		return err
	}


	err = db.SaveOrder(order)
	if err != nil {
		log.Printf("Failed to save order to database: %v", err)
		return err
	}


	if cache != nil {
		cache.Add(order)
		log.Printf("Order added to cache: %s", order.OrderUID)
	}

	log.Printf("Order processed successfully: %s", order.OrderUID)
	return nil
}

func (c *Consumer) Close() error {
	err := c.reader.Close()
	if err != nil {
		log.Printf("Failed to close Kafka consumer: %v", err)
		return err
	}
	log.Println("Kafka consumer closed successfully")
	return nil
}