package cache

import (
	"database/sql"
	"encoding/json"
	"log"
	"sync"
	"simple-kafka-app/models"
)

type OrderCache struct {
	mu     sync.RWMutex
	orders map[string]*models.Order
}

func NewOrderCache() *OrderCache {
	log.Println("Creating new order cache")
	return &OrderCache{
		orders: make(map[string]*models.Order),
	}
}

func (c *OrderCache) Add(order *models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.orders[order.OrderUID] = order
	log.Printf("Order added to cache: %s", order.OrderUID)
}

func (c *OrderCache) Get(orderUID string) (*models.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	order, exists := c.orders[orderUID]
	if exists {
		log.Printf("CACHE HIT for order: %s", orderUID)
	} else {
		log.Printf("CACHE MISS for order: %s", orderUID)
	}
	return order, exists
}

func (c *OrderCache) GetAll() []*models.Order {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	orders := make([]*models.Order, 0, len(c.orders))
	for _, order := range c.orders {
		orders = append(orders, order)
	}
	return orders
}

func (c *OrderCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return len(c.orders)
}

func (c *OrderCache) LoadFromDB(db *sql.DB) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	log.Println("Loading orders from database to cache...")
	
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction for cache loading: %v", err)
		return err
	}
	defer tx.Rollback() 

	rows, err := tx.Query(`
		SELECT 
			order_uid, track_number, entry, locale, internal_signature,
			customer_id, delivery_service, shardkey, sm_id, date_created,
			oof_shard, delivery_json, payment_json, items_json
		FROM orders 
		ORDER BY created_at DESC
	`)
	if err != nil {
		log.Printf("Failed to query orders from database: %v", err)
		return err
	}
	defer rows.Close()
	
	var count int
	for rows.Next() {
		var order models.Order
		var deliveryJSON, paymentJSON, itemsJSON []byte

		err := rows.Scan(
			&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale, &order.InternalSignature,
			&order.CustomerID, &order.DeliveryService, &order.Shardkey, &order.SmID, &order.DateCreated,
			&order.OofShard, &deliveryJSON, &paymentJSON, &itemsJSON,
		)
		if err != nil {
			log.Printf("Failed to scan order: %v", err)
			continue
		}
		if err := json.Unmarshal(deliveryJSON, &order.Delivery); err != nil {
			log.Printf("Failed to unmarshal delivery for order %s: %v", order.OrderUID, err)
			continue
		}
		if err := json.Unmarshal(paymentJSON, &order.Payment); err != nil {
			log.Printf("Failed to unmarshal payment for order %s: %v", order.OrderUID, err)
			continue
		}
		if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
			log.Printf("Failed to unmarshal items for order %s: %v", order.OrderUID, err)
			continue
		}
		
		c.orders[order.OrderUID] = &order
		count++
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v", err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit cache loading transaction: %v", err)
		return err
	}
	
	log.Printf("Loaded %d orders from database to cache", count)
	return nil
}

func (c *OrderCache) BulkAdd(orders []*models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	for _, order := range orders {
		c.orders[order.OrderUID] = order
		log.Printf("Order added to cache in bulk: %s", order.OrderUID)
	}
	
	log.Printf("Added %d orders to cache in bulk operation", len(orders))
}

func (c *OrderCache) Remove(orderUID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	delete(c.orders, orderUID)
	log.Printf("Order removed from cache: %s", orderUID)
}

func (c *OrderCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	count := len(c.orders)
	c.orders = make(map[string]*models.Order)
	log.Printf("Cache cleared: removed %d orders", count)
}

func (c *OrderCache) GetWithFallback(orderUID string, fallback func(string) (*models.Order, error)) (*models.Order, error) {
	if order, exists := c.Get(orderUID); exists {
		return order, nil
	}
	
	log.Printf("Cache miss for order: %s, calling fallback", orderUID)
	
	order, err := fallback(orderUID)
	if err != nil {
		return nil, err
	}

	c.Add(order)
	
	return order, nil
}