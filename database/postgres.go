package database

import (
	"database/sql"
	"encoding/json"
	"log"
	"simple-kafka-app/models"
	"time"
	_ "github.com/lib/pq"
)

type DB struct {
	*sql.DB
}

func ConnectPostgres(dsn string) (*DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("Failed to open PostgreSQL connection: %v", err)
		return nil, err
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	err = db.Ping()
	if err != nil {
		log.Printf("Failed to ping PostgreSQL: %v", err)
		return nil, err
	}

	log.Println("Successfully connected to PostgreSQL")
	return &DB{db}, nil
}

func (db *DB) InitSchema() error {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return err
	}
	defer tx.Rollback()

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

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return err
	}

	log.Println("Database schema initialized successfully")
	return nil
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
			items_json = EXCLUDED.items_json
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
			return nil, err
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

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit read transaction: %v", err)
		return nil, err
	}

	return orders, nil
}

// BulkSaveOrders для массового сохранения в одной транзакции
func (db *DB) BulkSaveOrders(orders []*models.Order) error {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin bulk transaction: %v", err)
		return err
	}
	defer tx.Rollback()

	for _, order := range orders {
		deliveryJSON, _ := json.Marshal(order.Delivery)
		paymentJSON, _ := json.Marshal(order.Payment)
		itemsJSON, _ := json.Marshal(order.Items)

		_, err := tx.Exec(`
			INSERT INTO orders (...) VALUES (...)
			ON CONFLICT (order_uid) DO UPDATE SET ...
		`, order.OrderUID, order.TrackNumber, order.Entry, order.Locale, order.InternalSignature,
			order.CustomerID, order.DeliveryService, order.Shardkey, order.SmID, order.DateCreated,
			order.OofShard, deliveryJSON, paymentJSON, itemsJSON,
		)
		
		if err != nil {
			log.Printf("Failed to save order %s: %v", order.OrderUID, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit bulk transaction: %v", err)
		return err
	}

	log.Printf("Bulk saved %d orders successfully", len(orders))
	return nil
}