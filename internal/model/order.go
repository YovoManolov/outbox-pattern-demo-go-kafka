package model

import (
	"time"

	"github.com/google/uuid"
)

// Order is the business entity we're persisting.
type Order struct {
	ID           uuid.UUID `json:"id"`
	CustomerName string    `json:"customer_name"`
	Item         string    `json:"item"`
	Amount       float64   `json:"amount"`
	CreatedAt    time.Time `json:"created_at"`
}

// OrderCreatedEvent is the payload we publish to Kafka whenever an order
// is created. Keeping event payloads explicit (rather than reusing the DB
// row directly) makes it easy to evolve the public contract independently
// of the internal schema later on.
type OrderCreatedEvent struct {
	OrderID      uuid.UUID `json:"order_id"`
	CustomerName string    `json:"customer_name"`
	Item         string    `json:"item"`
	Amount       float64   `json:"amount"`
	CreatedAt    time.Time `json:"created_at"`
}
