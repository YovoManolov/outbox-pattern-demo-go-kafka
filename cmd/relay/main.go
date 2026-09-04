// cmd/relay is the piece that actually makes the outbox pattern work: it
// polls the outbox table for unpublished rows and republishes them to
// Kafka. It's the only thing standing between "wrote to Postgres" and
// "the rest of the system finds out about it".
package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"outbox-demo/internal/store"
)

func main() {
	dsn := getEnv("DATABASE_URL", "postgres://outbox:outbox@localhost:5432/outbox_demo")
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9094"), ",")
	topic := getEnv("KAFKA_TOPIC", "order-events")
	pollInterval := 1 * time.Second

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	s := store.New(pool)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{}, // partition by key -> per-aggregate ordering
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	log.Printf("relay started, polling every %s, publishing to %s @ %v", pollInterval, topic, brokers)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for range ticker.C {
		drainOnce(ctx, s, writer)
	}
}

// drainOnce processes events one at a time until the outbox is empty,
// so a burst of writes gets flushed within a single tick instead of
// trickling out one per poll interval.
func drainOnce(ctx context.Context, s *store.Store, writer *kafka.Writer) {
	for {
		processed, err := s.ProcessNext(ctx, func(e store.OutboxEvent) error {
			if err := writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(e.AggregateID.String()),
				Value: e.Payload,
				Headers: []kafka.Header{
					{Key: "event_type", Value: []byte(e.EventType)},
					{Key: "aggregate_type", Value: []byte(e.AggregateType)},
				},
			}); err != nil {
				return err
			}
			log.Printf("published %s for %s %s", e.EventType, e.AggregateType, e.AggregateID)
			return nil
		})
		if err != nil {
			log.Printf("relay error: %v", err)
			return // back off until the next tick
		}
		if !processed {
			return // nothing left to do
		}
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
