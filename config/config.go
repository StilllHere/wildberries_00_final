package config

import (
	"log"
	"os"
)

type Config struct {
	KafkaBroker string
	PostgresDSN string
	KafkaTopic  string
}

func LoadConfig() *Config {
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")
	postgresDSN := getEnv("POSTGRES_DSN", "postgres://user:password@localhost:5433/myapp?sslmode=disable")
	kafkaTopic := getEnv("KAFKA_TOPIC", "orders")

	log.Printf("Loaded config - Kafka: %s, Topic: %s, Postgres: %s", 
		kafkaBroker, kafkaTopic, postgresDSN)
	
	return &Config{
		KafkaBroker: kafkaBroker,
		PostgresDSN: postgresDSN,
		KafkaTopic:  kafkaTopic,
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}