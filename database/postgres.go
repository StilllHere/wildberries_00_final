package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"simple-kafka-app/models"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type DB struct {
	*sql.DB
}

func ConnectPostgres(dsn string) (*DB, error) {
	var db *sql.DB
	var err error
	
	// Пытаемся подключиться с повторными попытками
	for i := 0; i < 5; i++ {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			log.Printf("Failed to open PostgreSQL connection (attempt %d/5): %v", i+1, err)
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}

		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(25)
		db.SetConnMaxLifetime(5 * time.Minute)

		err = db.Ping()
		if err == nil {
			log.Println("Successfully connected to PostgreSQL")
			return &DB{db}, nil
		}
		
		log.Printf("Failed to ping PostgreSQL (attempt %d/5): %v", i+1, err)
		db.Close()
		time.Sleep(time.Duration(i+1) * time.Second)
	}

	return nil, fmt.Errorf("failed to connect to PostgreSQL after 5 attempts: %v", err)
}

func (db *DB) InitSchema() error {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return err
	}
	defer tx.Rollback()

	// Таблица заказов
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			order_uid VARCHAR(255) PRIMARY KEY,
			track_number VARCHAR(255),
			entry VARCHAR(50),
			locale VARCHAR(10),
			internal_signature VARCHAR(255),
			customer_id VARCHAR(255),
			delivery_service VARCHAR(100),
			shardkey VARCHAR(10),
			sm_id INTEGER,
			date_created TIMESTAMP,
			oof_shard VARCHAR(10),
			delivery_json JSONB,
			payment_json JSONB,
			items_json JSONB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		log.Printf("Failed to create orders table: %v", err)
		return err
	}

	// Таблица для неудачных сообщений (Dead Letter Queue)
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS failed_messages (
			id SERIAL PRIMARY KEY,
			message_key TEXT NOT NULL,
			message_value TEXT NOT NULL,
			topic TEXT NOT NULL,
			error_message TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		log.Printf("Failed to create failed_messages table: %v", err)
		return err
	}

	// Индекс для быстрого поиска failed messages
	_, err = tx.Exec(`
		CREATE INDEX IF NOT EXISTS idx_failed_messages_created_at 
		ON failed_messages(created_at)
	`)
	if err != nil {
		log.Printf("Failed to create index on failed_messages: %v", err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return err
	}

	log.Println("Database schema initialized successfully")
	return nil
}

func (db *DB) SaveOrderWithRetry(order *models.Order, maxRetries int) error {
	var lastErr error
	
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := db.SaveOrder(order)
		if err == nil {
			log.Printf("Order %s saved successfully (attempt %d)", order.OrderUID, attempt)
			return nil
		}
		
		lastErr = err
		log.Printf("Failed to save order %s (attempt %d/%d): %v", 
			order.OrderUID, attempt, maxRetries, err)
		
		if attempt < maxRetries {
			backoff := time.Duration(attempt) * time.Second
			time.Sleep(backoff)
		}
	}
	
	return fmt.Errorf("failed to save order after %d attempts: %v", maxRetries, lastErr)
}

func (db *DB) SaveOrder(order *models.Order) error {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return err
	}
	defer tx.Rollback()

	deliveryJSON, err := json.Marshal(order.Delivery)
	if err != nil {
		log.Printf("Failed to marshal delivery: %v", err)
		return err
	}

	paymentJSON, err := json.Marshal(order.Payment)
	if err != nil {
		log.Printf("Failed to marshal payment: %v", err)
		return err
	}

	itemsJSON, err := json.Marshal(order.Items)
	if err != nil {
		log.Printf("Failed to marshal items: %v", err)
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO orders (
			order_uid, track_number, entry, locale, internal_signature,
			customer_id, delivery_service, shardkey, sm_id, date_created,
			oof_shard, delivery_json, payment_json, items_json
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (order_uid) DO UPDATE SET
			track_number = EXCLUDED.track_number,
			entry = EXCLUDED.entry,
			locale = EXCLUDED.locale,
			internal_signature = EXCLUDED.internal_signature,
			customer_id = EXCLUDED.customer_id,
			delivery_service = EXCLUDED.delivery_service,
			shardkey = EXCLUDED.shardkey,
			sm_id = EXCLUDED.sm_id,
			date_created = EXCLUDED.date_created,
			oof_shard = EXCLUDED.oof_shard,
			delivery_json = EXCLUDED.delivery_json,
			payment_json = EXCLUDED.payment_json,
			items_json = EXCLUDED.items_json,
			created_at = CURRENT_TIMESTAMP
	`,
		order.OrderUID, order.TrackNumber, order.Entry, order.Locale, order.InternalSignature,
		order.CustomerID, order.DeliveryService, order.Shardkey, order.SmID, order.DateCreated,
		order.OofShard, deliveryJSON, paymentJSON, itemsJSON,
	)
	
	if err != nil {
		log.Printf("Failed to save order to database: %v", err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return err
	}

	log.Printf("Order saved successfully: %s", order.OrderUID)
	return nil
}

func (db *DB) SaveFailedMessage(messageKey, messageValue, topic, errorMsg string) error {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction for failed message: %v", err)
		return err
	}
	defer tx.Rollback()

	// Обрезаем длинное сообщение для избежания ошибок
	if len(messageValue) > 10000 {
		messageValue = messageValue[:10000] + "... [truncated]"
	}

	_, err = tx.Exec(`
		INSERT INTO failed_messages (message_key, message_value, topic, error_message)
		VALUES ($1, $2, $3, $4)
	`, messageKey, messageValue, topic, errorMsg)

	if err != nil {
		log.Printf("Failed to save failed message to database: %v", err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit failed message transaction: %v", err)
		return err
	}

	log.Printf("Failed message saved to DLQ: %s", messageKey)
	return nil
}

func (db *DB) GetOrderByID(orderID string) (*models.Order, error) {
	var order models.Order
	var deliveryJSON, paymentJSON, itemsJSON []byte

	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin read transaction: %v", err)
		return nil, err
	}
	defer tx.Rollback()

	err = tx.QueryRow(`
		SELECT 
			order_uid, track_number, entry, locale, internal_signature,
			customer_id, delivery_service, shardkey, sm_id, date_created,
			oof_shard, delivery_json, payment_json, items_json
		FROM orders 
		WHERE order_uid = $1
	`, orderID).Scan(
		&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale, &order.InternalSignature,
		&order.CustomerID, &order.DeliveryService, &order.Shardkey, &order.SmID, &order.DateCreated,
		&order.OofShard, &deliveryJSON, &paymentJSON, &itemsJSON,
	)
	
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("order not found: %s", orderID)
		}
		log.Printf("Failed to query order: %v", err)
		return nil, err
	}

	if err := json.Unmarshal(deliveryJSON, &order.Delivery); err != nil {
		log.Printf("Failed to unmarshal delivery: %v", err)
		return nil, err
	}
	if err := json.Unmarshal(paymentJSON, &order.Payment); err != nil {
		log.Printf("Failed to unmarshal payment: %v", err)
		return nil, err
	}
	if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
		log.Printf("Failed to unmarshal items: %v", err)
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit read transaction: %v", err)
		return nil, err
	}

	return &order, nil
}

func (db *DB) GetAllOrders() ([]*models.Order, error) {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin read transaction: %v", err)
		return nil, err
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
		log.Printf("Failed to query orders: %v", err)
		return nil, err
	}
	defer rows.Close()

	var orders []*models.Order
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
			log.Printf("Failed to unmarshal delivery: %v", err)
			continue
		}
		if err := json.Unmarshal(paymentJSON, &order.Payment); err != nil {
			log.Printf("Failed to unmarshal payment: %v", err)
			continue
		}
		if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
			log.Printf("Failed to unmarshal items: %v", err)
			continue
		}

		orders = append(orders, &order)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v", err)
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit read transaction: %v", err)
		return nil, err
	}

	return orders, nil
}

func (db *DB) HealthCheck() error {
	return db.Ping()
}

func (db *DB) GetFailedMessages(limit int) ([]map[string]interface{}, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.Query(`
		SELECT id, message_key, topic, error_message, created_at
		FROM failed_messages 
		ORDER BY created_at DESC 
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []map[string]interface{}
	for rows.Next() {
		var id int
		var messageKey, topic, errorMessage string
		var createdAt time.Time

		err := rows.Scan(&id, &messageKey, &topic, &errorMessage, &createdAt)
		if err != nil {
			return nil, err
		}

		messages = append(messages, map[string]interface{}{
			"id":            id,
			"message_key":   messageKey,
			"topic":         topic,
			"error_message": errorMessage,
			"created_at":    createdAt,
		})
	}

	return messages, nil
}

func IsDatabaseConnectionError(err error) bool {
	if err == nil {
		return false
	}
	
	errorMsg := strings.ToLower(err.Error())
	return strings.Contains(errorMsg, "connection") ||
		strings.Contains(errorMsg, "connect") ||
		strings.Contains(errorMsg, "network") ||
		strings.Contains(errorMsg, "closed") ||
		strings.Contains(errorMsg, "refused") ||
		strings.Contains(errorMsg, "timeout")
}