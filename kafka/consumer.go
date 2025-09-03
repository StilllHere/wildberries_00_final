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
		log.Printf("ERROR: Failed to parse order message (Key: %s). Error: %v. Message will be SKIPPED.", string(msg.Key), err)
		return nil
	}

	if !c.isOrderValid(order) {
		log.Printf("ERROR: Order %s is incomplete or invalid. Message will be SKIPPED.", order.OrderUID)
		return nil
	}

	err = db.SaveOrder(order)
	if err != nil {
		log.Printf("CRITICAL: Failed to save order %s to database: %v", order.OrderUID, err)
		return err
	}

	if cache != nil {
		cache.Add(order)
		log.Printf("Order successfully processed and cached: %s", order.OrderUID)
	} else {
		log.Printf("Order successfully processed (cache is nil): %s", order.OrderUID)
	}

	return nil
}

// комплексная проверка заказа на валидность
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
	if order.Delivery.Phone == "" {
		log.Printf("VALIDATION ERROR: Delivery.Phone is empty for order %s", order.OrderUID)
		return false
	}
	if order.Delivery.Address == "" {
		log.Printf("VALIDATION ERROR: Delivery.Address is empty for order %s", order.OrderUID)
		return false
	}
	if order.Delivery.City == "" {
		log.Printf("VALIDATION ERROR: Delivery.City is empty for order %s", order.OrderUID)
		return false
	}
	if order.Payment.Transaction == "" {
		log.Printf("VALIDATION ERROR: Payment.Transaction is empty for order %s", order.OrderUID)
		return false
	}
	if order.Payment.Currency == "" {
		log.Printf("VALIDATION ERROR: Payment.Currency is empty for order %s", order.OrderUID)
		return false
	}
	if order.Payment.Provider == "" {
		log.Printf("VALIDATION ERROR: Payment.Provider is empty for order %s", order.OrderUID)
		return false
	}
	if order.Payment.Amount <= 0 {
		log.Printf("VALIDATION ERROR: Payment.Amount is invalid (%d) for order %s", order.Payment.Amount, order.OrderUID)
		return false
	}
	if len(order.Items) == 0 {
		log.Printf("VALIDATION ERROR: No items in order %s", order.OrderUID)
		return false
	}
	for i, item := range order.Items {
		if item.Name == "" {
			log.Printf("VALIDATION ERROR: Item[%d].Name is empty in order %s", i, order.OrderUID)
			return false
		}
		if item.Price <= 0 {
			log.Printf("VALIDATION ERROR: Item[%d].Price is invalid (%d) in order %s", i, item.Price, order.OrderUID)
			return false
		}
		if item.TotalPrice <= 0 {
			log.Printf("VALIDATION ERROR: Item[%d].TotalPrice is invalid (%d) in order %s", i, item.TotalPrice, order.OrderUID)
			return false
		}
	}

	log.Printf("Order validation passed: %s", order.OrderUID)
	return true
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