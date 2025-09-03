package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"simple-kafka-app/api"
	"simple-kafka-app/cache"
	"simple-kafka-app/config"
	"simple-kafka-app/database"
	"simple-kafka-app/kafka"
)

func main() {
	log.Println("Starting order processing application with cache...")
	cfg := config.LoadConfig()

	db, err := database.ConnectPostgres(cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	err = db.InitSchema()
	if err != nil {
		log.Fatalf("Failed to initialize database schema: %v", err)
	}

	orderCache := cache.NewOrderCache()
	log.Printf("Initial cache size: %d", orderCache.Size())

	log.Println("Waiting 5 seconds before loading cache...")
	time.Sleep(5 * time.Second)

	err = orderCache.LoadFromDB(db.DB)
	if err != nil {
		log.Printf("Warning: Failed to load cache from DB: %v", err)
	} else {
		log.Printf("Cache initialized with %d orders", orderCache.Size())
	}


	consumer := kafka.NewConsumer(cfg.KafkaBroker, cfg.KafkaTopic, "order-processor-group")
	defer consumer.Close()

	server := api.NewServer(orderCache, db)
	go func() {
		if err := server.Start(":8080"); err != nil {
			log.Fatalf("Failed to start API server: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)


	go processOrders(ctx, consumer, db, orderCache)

	log.Println("Application started successfully!")
	log.Println("Web interface: http://localhost:8080")
	log.Println("Orders API: http://localhost:8080/orders")
	log.Println("Order by ID: http://localhost:8080/orders/{id}")
	log.Println("Cache status: http://localhost:8080/cache-status")
	log.Println("Press Ctrl+C to stop.")

	<-sigChan
	log.Println("Shutting down application...")
}

func processOrders(ctx context.Context, consumer *kafka.Consumer, db *database.DB, cache *cache.OrderCache) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping order processing")
			return
		default:
			err := consumer.ReadAndProcessOrder(ctx, db, cache)
			if err != nil {
				log.Printf("Error processing order: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}
		}
	}
}