// cmd/consumer is not part of the outbox pattern itself - it's a stand-in
// for "some other service" that cares about order events. Run it alongside
// the api and relay to watch events flow end-to-end.
package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9094"), ",")
	topic := getEnv("KAFKA_TOPIC", "order-events")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: "demo-consumer",
	})
	defer reader.Close()

	log.Printf("consumer listening on %s @ %v", topic, brokers)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("read message: %v", err)
		}
		log.Printf("received: key=%s value=%s", msg.Key, msg.Value)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
