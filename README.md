# Outbox Pattern Demo (Go + Kafka + Postgres)

A minimal, runnable implementation of the **transactional outbox pattern**:
how to reliably publish an event to Kafka *only if* a related database write
succeeds — without a distributed transaction and without ever losing an event.

## The problem it solves

If your code does this:

```go
db.Save(order)
kafka.Publish(orderCreatedEvent)  // <- what if this line fails or the process dies here?
```

...you can end up with an order that exists in Postgres but was never
announced to the rest of your system (or the reverse, if you publish first).
There's no atomic way to "commit to two systems at once."

## The fix: write the event to the database too

Instead of publishing directly, write the event into an `outbox_events`
table **in the same database transaction** as the business write. A separate
background process (the *relay*) polls that table and republishes anything
it finds to Kafka, then marks it done.

```
┌─────────────┐        1 tx: insert order        ┌────────────┐
│   api        │ ────────────────────────────────▶│  Postgres   │
│ (cmd/api)    │        + insert outbox_event      │             │
└─────────────┘                                    └─────┬──────┘
                                                           │ poll (FOR UPDATE SKIP LOCKED)
                                                           ▼
                                                    ┌─────────────┐
                                                    │   relay      │
                                                    │ (cmd/relay)  │
                                                    └──────┬───────┘
                                                           │ publish
                                                           ▼
                                                    ┌─────────────┐
                                                    │   Kafka      │
                                                    └──────┬───────┘
                                                           │ consume
                                                           ▼
                                                    ┌─────────────┐
                                                    │  consumer    │
                                                    │(cmd/consumer)│
                                                    └─────────────┘
```

Because the order row and the outbox row are written in one transaction,
either both exist or neither does — there's no window where an order is
saved but its event is lost. The relay guarantees **at-least-once**
delivery to Kafka (it's possible for an event to be published twice if the
relay crashes between the Kafka write and the `UPDATE ... SET published_at`,
so consumers should be idempotent).

## Project layout

```
cmd/api/        HTTP service — POST /orders creates an order + outbox event
cmd/relay/      Polls outbox_events and publishes unpublished rows to Kafka
cmd/consumer/   Demo consumer that prints events as they arrive
internal/store/ The transactional write + the claim-publish-mark logic
internal/model/ Order + event types
db/init.sql     Schema for orders and outbox_events
docker-compose.yml   Kafka (KRaft, single node) + Postgres + Kafka UI
```

## Running it

```bash
# 1. Start Kafka + Postgres
docker compose up -d

# 2. Install Go deps
go mod tidy

# 3. In separate terminals:
go run ./cmd/relay
go run ./cmd/consumer
go run ./cmd/api

# 4. Create an order
curl -X POST localhost:8081/orders \
  -H 'content-type: application/json' \
  -d '{"customer_name":"Ada","item":"Mechanical Keyboard","amount":129.99}'
```

You should see the relay log `published order.created for order ...` and
the consumer log `received: key=... value=...` within a second.

Kafka UI is available at http://localhost:8080 if you want to browse the
`order-events` topic visually.

## What to try next

- Kill the relay mid-poll and restart it — no events are lost.
- Run two relay instances at once — `FOR UPDATE SKIP LOCKED` keeps them
  from double-processing the same row.
- Swap the polling relay for Debezium reading the Postgres WAL, for a
  CDC-based version of the same pattern with lower latency.
