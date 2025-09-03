package kafka

import (
	"context"
	"fmt"
	"log"
	"simple-kafka-app/database"
	"simple-kafka-app/cache"
	"simple-kafka-app/models"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader    *kafkago.Reader
	db        *database.DB
	cache     *cache.OrderCache
	topic     string
	groupID   string
	isPaused  bool
	pauseUntil time.Time
}

func NewConsumer(broker string, topic string, groupID string, db *database.DB, cache *cache.OrderCache) *Consumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:          []string{broker},
		Topic:            topic,
		GroupID:          groupID,
		MinBytes:         10e3,
		MaxBytes:         10e6,
		MaxWait:          1 * time.Second,
		ReadLagInterval:  -1,
		RetentionTime:    24 * time.Hour * 7,
	})

	log.Printf("Kafka consumer created for topic: %s, group: %s", topic, groupID)
	
	return &Consumer{
		reader:  reader,
		db:      db,
		cache:   cache,
		topic:   topic,
		groupID: groupID,
	}
}

func (c *Consumer) StartProcessing(ctx context.Context) {
	log.Println("Starting Kafka consumer processing loop...")
	
	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping consumer due to context cancellation")
			return
		default:
			if c.isPaused {
				if time.Now().After(c.pauseUntil) {
					c.isPaused = false
					log.Println("Consumer resumed after cooldown period")
				} else {
					time.Sleep(1 * time.Second)
					continue
				}
			}

			err := c.processSingleMessage(ctx)
			if err != nil {
				log.Printf("Error processing message: %v", err)
				if database.IsDatabaseConnectionError(err) {
					c.pauseConsumer(30 * time.Second)
				}
			}
		}
	}
}

func (c *Consumer) processSingleMessage(ctx context.Context) error {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch message: %v", err)
	}

	log.Printf("Message received from Kafka - Key: %s", string(msg.Key))

	processingErr := c.processMessageContent(msg)
	
	if processingErr != nil {
		dlqErr := c.db.SaveFailedMessage(
			string(msg.Key),
			string(msg.Value),
			c.topic,
			processingErr.Error(),
		)
		if dlqErr != nil {
			log.Printf("Failed to save message to DLQ: %v", dlqErr)
		}
		
		log.Printf("Message processing failed and saved to DLQ: %v", processingErr)
		return processingErr
	}

	err = c.reader.CommitMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to commit message: %v", err)
	}

	log.Printf("Message successfully processed and committed: %s", string(msg.Key))
	return nil
}

func (c *Consumer) processMessageContent(msg kafkago.Message) error {
	order, err := models.ParseOrderFromJSON(msg.Value)
	if err != nil {
		return fmt.Errorf("failed to parse JSON: %v", err)
	}

	if !c.isOrderValid(order) {
		return fmt.Errorf("order validation failed for: %s", order.OrderUID)
	}
	err = c.db.SaveOrderWithRetry(order, 3)
	if err != nil {
		return fmt.Errorf("failed to save order after retries: %v", err)
	}
	if c.cache != nil {
		c.cache.Add(order)
	}

	return nil
}

func (c *Consumer) isOrderValid(order *models.Order) bool {
	if order.OrderUID == "" {
		log.Printf("VALIDATION ERROR: OrderUID is empty")
		return false
	}
	if order.TrackNumber == "" {
		log.Printf("VALIDATION ERROR: TrackNumber is empty for order %s", order.OrderUID)
		return false
	}
	if order.Entry == "" {
		log.Printf("VALIDATION ERROR: Entry is empty for order %s", order.OrderUID)
		return false
	}
	if order.Delivery.Name == "" {
		log.Printf("VALIDATION ERROR: Delivery.Name is empty for order %s", order.OrderUID)
		return false
	}
	if len(order.Items) == 0 {
		log.Printf("VALIDATION ERROR: No items in order %s", order.OrderUID)
		return false
	}
	
	return true
}

func (c *Consumer) pauseConsumer(duration time.Duration) {
	c.isPaused = true
	c.pauseUntil = time.Now().Add(duration)
	log.Printf("Consumer paused for %v due to database connection issues", duration)
}

func (c *Consumer) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"topic":       c.topic,
		"group_id":    c.groupID,
		"is_paused":   c.isPaused,
		"paused_until": c.pauseUntil,
		"status":      "active",
	}
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