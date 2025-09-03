package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

func testPostgres() {
	log.Println("Testing PostgreSQL connection on port 5433...")
	
	db, err := sql.Open("postgres", "postgres://user:password@localhost:5433/myapp?sslmode=disable")
	if err != nil {
		log.Printf("PostgreSQL connection failed: %v", err)
		return
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Printf("PostgreSQL ping failed: %v", err)
		return
	}


	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_connection (
			id SERIAL PRIMARY KEY,
			test_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		log.Printf("PostgreSQL create table failed: %v", err)
		return
	}


	_, err = db.Exec("INSERT INTO test_connection DEFAULT VALUES")
	if err != nil {
		log.Printf("PostgreSQL insert failed: %v", err)
		return
	}

	log.Println("PostgreSQL connection successful!")
}

func testKafka() {
	log.Println("Testing Kafka connection...")
	
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Printf("Kafka connection failed: %v", err)
		return
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		log.Printf("Kafka controller failed: %v", err)
		return
	}

	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.Printf("Kafka read partitions failed: %v", err)
		return
	}

	log.Printf("Kafka connection successful!")
	log.Printf("   Controller: %s:%d", controller.Host, controller.Port)
	log.Printf("   Available partitions: %d", len(partitions))
}

func testKafkaTopics() {
	log.Println("Testing Kafka topics...")
	
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Printf("Kafka topics connection failed: %v", err)
		return
	}
	defer conn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             "orders",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		log.Printf("Kafka create topics warning: %v", err)
	}

	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.Printf("Kafka read partitions failed: %v", err)
		return
	}

	log.Println("Kafka topics available:")
	for _, partition := range partitions {
		log.Printf("   - %s", partition.Topic)
	}
}

func main() {
	log.Println("Running connection tests...")
	log.Println("==========================================")
	
	testPostgres()
	time.Sleep(1 * time.Second)
	
	testKafka()
	time.Sleep(1 * time.Second)
	
	testKafkaTopics()
	
	log.Println("==========================================")
	log.Println("Tests completed")
	time.Sleep(2 * time.Second)
}