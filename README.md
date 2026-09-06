# Outbox Pattern Demo (Go + Kafka + Postgres)

A minimal, runnable implementation of the **transactional outbox pattern**:
how to reliably publish an event to Kafka *only if* a related database write
succeeds - without a distributed transaction and without ever losing an event.

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
either both exist or neither does - there's no window where an order is
saved but its event is lost. The relay guarantees **at-least-once**
delivery to Kafka (it's possible for an event to be published twice if the
relay crashes between the Kafka write and the `UPDATE ... SET published_at`,
so consumers should be idempotent).

## Project layout

```
cmd/api/        HTTP service - POST /orders creates an order + outbox event
cmd/relay/      Polls outbox_events and publishes unpublished rows to Kafka
cmd/consumer/   Demo consumer that prints events as they arrive
internal/store/ The transactional write + the claim-publish-mark logic
internal/model/ Order + event types
db/init.sql     Schema for orders and outbox_events
docker-compose.yml   Kafka (KRaft, single node) + Postgres + Kafka UI
scripts/ordering-test.sh   Measures the SKIP LOCKED ordering caveat
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

- Kill the relay mid-poll and restart it - no events are lost.
- Run two relay instances at once - `FOR UPDATE SKIP LOCKED` keeps them
  from double-processing the same row (but see the ordering caveat below).
- Swap the polling relay for Debezium reading the Postgres WAL, for a
  CDC-based version of the same pattern with lower latency.

## The ordering caveat (and how to measure it)

`FOR UPDATE SKIP LOCKED` lets you add relay instances without electing a
leader, and no row is ever processed twice concurrently. What it does *not*
give you is **per-aggregate ordering**.

Because each relay skips rows another has locked, three relays draining
events 1, 2, 3 for the same aggregate end up holding one row each - and then
publish concurrently. Whichever `WriteMessages` call reaches the broker
first lands first:

```
Relay A: claims seq 1 (locks it)
Relay B: seq 1 locked -> SKIPS -> claims seq 2
Relay C: seq 1,2 locked -> SKIPS -> claims seq 3
         ...all three publish at once; arrival order is a race
```

This matters here because the relay keys messages by aggregate ID
(`kafka.Hash{}`), so every event for one order lands on a single partition.
Kafka preserves order *within* a partition - but only in arrival order, so
it faithfully preserves whatever sequence the relays produced.

`scripts/ordering-test.sh` measures it. It seeds N sequenced events for a
single aggregate, drains them with several concurrent relays, then reads the
topic back and counts inversions:

```bash
./scripts/ordering-test.sh 120 3    # [num_events] [num_relays]
```

```
==> Order events reached Kafka in (first 40):
3 2 1 4 5 6 7 8 10 9 11 12 14 13 15 16 19 18 17 20 23 22 21 24 25 26 27 ...

    published:      120
    duplicates:     0
    out of order:   33
    relays:         4 (3 started here, 30 events published by pre-existing relays)
```

`duplicates: 0` is `SKIP LOCKED` doing its job; `out of order: 33` is what it
costs. The exact count varies between runs - it's a race, not a deterministic
result - but the shape is consistent: events land one to three positions away
from where they should be, because relays claim adjacent rows microseconds
apart and then race over a few milliseconds of network.

The race is also load-dependent. At low volume (say 40 events across 2
relays) the test often reports `0`, which is what makes this an unpleasant
production surprise rather than something a smoke test catches.

Note the `relays:` line. The second argument is how many relays the script
starts itself, but any relay you already have running (your `go run
./cmd/relay` terminal, for instance) competes for the same rows, so the real
concurrency is usually one higher. The script detects this by counting how
many events its own relays published and attributing the rest.

The script uses a dedicated `probe.seq` event type and a fixed probe UUID; it
clears that data at the start of each run, so to tidy up before taking
screenshots of the main demo:

```bash
docker exec outbox-postgres psql -U outbox -d outbox_demo -c "
DELETE FROM outbox_events WHERE event_type='probe.seq';
DELETE FROM orders WHERE id='11111111-1111-1111-1111-111111111111';"
```

If you need ordering, the options are:

- **Claim one aggregate at a time** rather than the next row - lock the
  aggregate so all its events drain through a single relay in sequence. You
  keep parallelism across orders and lose it only within one, which is
  usually exactly the tradeoff you want.
- **Shard the outbox** by hashing `aggregate_id` and give each relay a slice.
- **Stay single-relay** and accept the throughput ceiling (what this demo
  does by default).
