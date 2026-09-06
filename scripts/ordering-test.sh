#!/usr/bin/env bash
# Demonstrates the ordering caveat of FOR UPDATE SKIP LOCKED.
#
# Seeds N sequenced events for a SINGLE aggregate, drains them with several
# concurrent relays, then checks what order they actually reached Kafka in.
#
# Expected result: zero duplicates (SKIP LOCKED works), but out-of-order
# arrivals, because each relay skips locked rows and publishes concurrently.
#
# Usage:  ./scripts/ordering-test.sh [num_events] [num_relays]
set -euo pipefail

EVENTS=${1:-150}
RELAYS=${2:-3}
AGG='11111111-1111-1111-1111-111111111111'
PSQL="docker exec outbox-postgres psql -U outbox -d outbox_demo -tAc"
KAFKA="docker exec outbox-kafka"
TMP=$(mktemp -d)
trap 'pkill -f "$TMP/relay" 2>/dev/null || true; rm -rf "$TMP"' EXIT

echo "==> Building relay"
go build -o "$TMP/relay" ./cmd/relay

echo "==> Cleaning previous probe data"
$PSQL "DELETE FROM outbox_events WHERE event_type='probe.seq';
       DELETE FROM orders WHERE id='$AGG';" >/dev/null

START=$($KAFKA /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic order-events 2>/dev/null | cut -d: -f3)
echo "    topic offset before test: $START"

echo "==> Starting $RELAYS extra relay instances"
for i in $(seq 1 "$RELAYS"); do "$TMP/relay" >"$TMP/relay$i.log" 2>&1 & done
sleep 2

echo "==> Seeding $EVENTS sequenced events for one aggregate"
$PSQL "INSERT INTO orders (id, customer_name, item, amount)
       VALUES ('$AGG','OrderingProbe','probe',1.00) ON CONFLICT (id) DO NOTHING;
       INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, created_at)
       SELECT 'order','$AGG','probe.seq', json_build_object('seq', g)::jsonb,
              now() + (g * interval '1 microsecond')
       FROM generate_series(1,$EVENTS) g;" >/dev/null

echo "==> Draining"
for _ in $(seq 1 60); do
  PENDING=$($PSQL "SELECT count(*) FROM outbox_events
                   WHERE published_at IS NULL AND event_type='probe.seq';")
  [ "$PENDING" = "0" ] && break
  sleep 1
done
echo "    all $EVENTS events published"

SEQ=$($KAFKA /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order-events \
  --partition 0 --offset "$START" --max-messages "$EVENTS" --timeout-ms 20000 2>/dev/null \
  | grep -o '"seq": *[0-9]*' | grep -o '[0-9]*')

echo
echo "==> Order events reached Kafka in (first 40):"
echo "$SEQ" | head -40 | tr '\n' ' '; echo
echo
# Count what this script's own relays published. Anything left over was
# published by a relay that was already running (e.g. your `go run
# ./cmd/relay` terminal), which competes in the test too.
MINE=0
for i in $(seq 1 "$RELAYS"); do
  N=$(grep -c 'published probe.seq' "$TMP/relay$i.log" || true)
  MINE=$((MINE + N))
done
OTHER=$((EVENTS - MINE))
TOTAL=$RELAYS
[ "$OTHER" -gt 0 ] && TOTAL=$((RELAYS + 1))

echo "$SEQ" | awk -v n="$EVENTS" '
  NR==1 {max=$1; next}
  { if ($1 < max) inv++; else max=$1 }
  END {
    printf "    published:      %d\n", NR
    printf "    duplicates:     %d\n", NR - n
    printf "    out of order:   %d\n", inv+0
  }'
echo "    relays:         $TOTAL ($RELAYS started here, $OTHER events published by pre-existing relays)"
