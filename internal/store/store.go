package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"outbox-demo/internal/model"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// CreateOrderWithOutbox is the heart of the pattern: the order row and the
// outbox event row are written in a single database transaction. Either
// both commit or neither does - there is no window where an order exists
// without a corresponding "order created" event waiting to be published.
func (s *Store) CreateOrderWithOutbox(ctx context.Context, o model.Order) (model.Order, error) {
	o.ID = uuid.New()
	o.CreatedAt = time.Now().UTC()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return o, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, customer_name, item, amount, created_at)
		VALUES ($1, $2, $3, $4, $5)`,
		o.ID, o.CustomerName, o.Item, o.Amount, o.CreatedAt,
	)
	if err != nil {
		return o, fmt.Errorf("insert order: %w", err)
	}

	event := model.OrderCreatedEvent{
		OrderID:      o.ID,
		CustomerName: o.CustomerName,
		Item:         o.Item,
		Amount:       o.Amount,
		CreatedAt:    o.CreatedAt,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return o, fmt.Errorf("marshal event: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
		VALUES ($1, $2, $3, $4)`,
		"order", o.ID, "order.created", payload,
	)
	if err != nil {
		return o, fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return o, fmt.Errorf("commit tx: %w", err)
	}

	return o, nil
}

// OutboxEvent is a row from the outbox table, as seen by the relay.
type OutboxEvent struct {
	ID            uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	Payload       []byte
	CreatedAt     time.Time
}

// ProcessNext claims a single unpublished event, hands it to publish(), and
// marks it published - all inside one transaction. Locking, publishing, and
// marking-done happen together so there's no gap where a crash could lose
// track of whether an event was sent.
//
// FOR UPDATE SKIP LOCKED means multiple relay instances can run
// concurrently: each one skips rows another instance already has locked
// instead of blocking on them, so you can scale the relay horizontally.
//
// Returns (false, nil) when there is nothing left to process.
func (s *Store) ProcessNext(ctx context.Context, publish func(OutboxEvent) error) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	var e OutboxEvent
	err = tx.QueryRow(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
	).Scan(&e.ID, &e.AggregateType, &e.AggregateID, &e.EventType, &e.Payload, &e.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("claim next event: %w", err)
	}

	if err := publish(e); err != nil {
		return false, fmt.Errorf("publish event %s: %w", e.ID, err)
	}

	if _, err := tx.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE id = $1`, e.ID); err != nil {
		return false, fmt.Errorf("mark published: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit tx: %w", err)
	}
	return true, nil
}
