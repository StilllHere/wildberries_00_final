package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"simple-kafka-app/kafka"
)

func main() {
	log.Println("Starting universal test order producer...")

	producer := kafka.NewProducer("localhost:9092", "orders")
	defer producer.Close()

	// Стандартный тестовый заказ
	log.Println("Sending standard test order...")
	sendStandardTestOrder(producer)
	time.Sleep(1 * time.Second)

	// Заказы семьи Аддамс (валидные)
	log.Println("Sending Addams Family orders...")
	sendAddamsFamilyOrders(producer)
	time.Sleep(1 * time.Second)

	// Невалидные заказы
	log.Println("Sending invalid orders from Cousin Itt...")
	sendInvalidOrders(producer)

	log.Println("All test orders sent successfully!")
}

// Стандартный тестовый заказ
func sendStandardTestOrder(producer *kafka.Producer) {
	testOrder := map[string]interface{}{
		"order_uid": "b563feb7b2b84b6test",
		"track_number": "WBILMTESTTRACK",
		"entry": "WBIL",
		"delivery": map[string]interface{}{
			"name":    "Test Testov",
			"phone":   "+9720000000",
			"zip":     "2639809",
			"city":    "Kiryat Mozkin",
			"address": "Ploshad Mira 15",
			"region":  "Kraiot",
			"email":   "test@gmail.com",
		},
		"payment": map[string]interface{}{
			"transaction":   "b563feb7b2b84b6test",
			"request_id":    "",
			"currency":      "USD",
			"provider":      "wbpay",
			"amount":        1817,
			"payment_dt":    1637907727,
			"bank":          "alpha",
			"delivery_cost": 1500,
			"goods_total":   317,
			"custom_fee":    0,
		},
		"items": []map[string]interface{}{
			{
				"chrt_id":      9934930,
				"track_number": "WBILMTESTTRACK",
				"price":        453,
				"rid":          "ab4219087a764ae0btest",
				"name":         "Mascaras",
				"sale":         30,
				"size":         "0",
				"total_price":  317,
				"nm_id":        2389212,
				"brand":        "Vivienne Sabo",
				"status":       202,
			},
		},
		"locale":            "en",
		"internal_signature": "",
		"customer_id":       "test",
		"delivery_service":  "meest",
		"shardkey":          "9",
		"sm_id":             99,
		"date_created":      "2021-11-26T06:22:19Z",
		"oof_shard":         "1",
	}

	sendOrder(producer, testOrder, "Standard Test Order")
}


func sendAddamsFamilyOrders(producer *kafka.Producer) {
	addamsOrders := []map[string]interface{}{
		{
			"order_uid":    "addams_gomez_" + randString(6),
			"track_number": "ADDAMS001",
			"entry":        "ADDAMS",
			"delivery": map[string]interface{}{
				"name":    "Gomez Addams",
				"phone":   "+16666666666",
				"zip":     "66666",
				"city":    "Westfield",
				"address": "Cemetery Lane 666",
				"region":  "New Jersey",
				"email":   "gomez.addams@addamsfamily.com",
			},
			"payment": map[string]interface{}{
				"transaction":   "trans_gomez_" + randString(8),
				"request_id":    "",
				"currency":      "USD",
				"provider":      "addams_pay",
				"amount":        6666,
				"payment_dt":    time.Now().Unix(),
				"bank":          "Cursed Bank",
				"delivery_cost": 666,
				"goods_total":   6000,
				"custom_fee":    0,
			},
			"items": []map[string]interface{}{
				{
					"chrt_id":      6666666,
					"track_number": "ADDAMS001",
					"price":        2000,
					"rid":          "sword_antique",
					"name":         "Antique Sword",
					"sale":         0,
					"size":         "L",
					"total_price":  2000,
					"nm_id":        666001,
					"brand":        "Addams Armory",
					"status":       202,
				},
			},
			"locale":            "en",
			"internal_signature": "",
			"customer_id":       "gomez_addams",
			"delivery_service":  "Thing Express",
			"shardkey":          "6",
			"sm_id":             66,
			"date_created":      time.Now().Format(time.RFC3339),
			"oof_shard":         "6",
		},
		{
			"order_uid":    "addams_morticia_" + randString(6),
			"track_number": "ADDAMS002",
			"entry":        "ADDAMS",
			"delivery": map[string]interface{}{
				"name":    "Morticia Addams",
				"phone":   "+16666666667",
				"zip":     "66666",
				"city":    "Westfield",
				"address": "Cemetery Lane 666",
				"region":  "New Jersey",
				"email":   "morticia.addams@addamsfamily.com",
			},
			"payment": map[string]interface{}{
				"transaction":   "trans_morticia_" + randString(8),
				"request_id":    "",
				"currency":      "USD",
				"provider":      "addams_pay",
				"amount":        4444,
				"payment_dt":    time.Now().Unix(),
				"bank":          "Cursed Bank",
				"delivery_cost": 444,
				"goods_total":   4000,
				"custom_fee":    0,
			},
			"items": []map[string]interface{}{
				{
					"chrt_id":      6666668,
					"track_number": "ADDAMS002",
					"price":        2500,
					"rid":          "black_dress",
					"name":         "Black Velvet Dress",
					"sale":         0,
					"size":         "S",
					"total_price":  2500,
					"nm_id":        666003,
					"brand":        "Morticia Couture",
					"status":       202,
				},
			},
			"locale":            "en",
			"internal_signature": "",
			"customer_id":       "morticia_addams",
			"delivery_service":  "Thing Express",
			"shardkey":          "6",
			"sm_id":             66,
			"date_created":      time.Now().Format(time.RFC3339),
			"oof_shard":         "6",
		},
	}

	for _, order := range addamsOrders {
		sendOrder(producer, order, "Addams Family")
		time.Sleep(500 * time.Millisecond)
	}
}


func sendInvalidOrders(producer *kafka.Producer) {
	// Нет delivery, payment, items (невалидные данные)
	incompleteOrder := map[string]interface{}{
		"order_uid":    "invalid_incomplete_" + randString(6),
		"track_number": "INVALID001",
	}
	// Неправильные типы данных
	wrongTypesOrder := map[string]interface{}{
		"order_uid":    "invalid_types_" + randString(6),
		"track_number": "INVALID002",
		"delivery": map[string]interface{}{
			"name":    "Cousin Itt",
			"phone":   "+16666666660",
			"zip":     "66666",
			"city":    "Westfield",
			"address": "Cemetery Lane 666",
			"region":  "New Jersey",
			"email":   "cousin.itt@addamsfamily.com",
		},
		"payment": map[string]interface{}{
			"transaction":   "trans_itt_" + randString(8),
			"request_id":    "",
			"currency":      "USD",
			"provider":      "addams_pay",
			"amount":        "should_be_number", 
			"payment_dt":    time.Now().Unix(),
			"bank":          "Cursed Bank",
			"delivery_cost": 123,
			"goods_total":   456,
			"custom_fee":    0,
		},
		"items": []map[string]interface{}{},
	}

	brokenJSON := `{"order_uid": "invalid_broken", "track_number": "INVALID003", "delivery": {"name": "Test", "email": "test@test.com"` 
	emptyOrder := map[string]interface{}{}

	sendOrder(producer, incompleteOrder, "Invalid (incomplete)")
	time.Sleep(500 * time.Millisecond)
	
	sendOrder(producer, wrongTypesOrder, "Invalid (wrong types)")
	time.Sleep(500 * time.Millisecond)
	
	sendRawMessage(producer, "invalid_broken", brokenJSON)
	time.Sleep(500 * time.Millisecond)
	
	sendOrder(producer, emptyOrder, "Invalid (empty)")
}

func sendOrder(producer *kafka.Producer, order map[string]interface{}, orderType string) {
	orderJSON, err := json.Marshal(order)
	if err != nil {
		log.Printf("Failed to marshal %s order: %v", orderType, err)
		return
	}

	orderUID, ok := order["order_uid"].(string)
	if !ok {
		orderUID = "unknown_" + randString(6)
	}

	err = producer.SendMessage(context.Background(), orderUID, string(orderJSON))
	if err != nil {
		log.Printf("Failed to send %s order: %v", orderType, err)
		return
	}

	log.Printf("Sent %s order: %s", orderType, orderUID)
}

func sendRawMessage(producer *kafka.Producer, key string, message string) {
	err := producer.SendMessage(context.Background(), key, message)
	if err != nil {
		log.Printf("Failed to send raw message: %v", err)
		return
	}
	log.Printf("Sent raw message: %s", key)
}

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}