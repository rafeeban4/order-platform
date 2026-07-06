# order-platform

Event-driven order processing pipeline with **published, reproducible load numbers**: Spring Boot ingest → Redpanda/Kafka → consumer-group workers → Postgres, traced end-to-end with OpenTelemetry.

```
k6 ──► ingest-api ──► orders.v1 (3 partitions) ──► order-worker ×N ──► Postgres
        (202+ULID)         Redpanda                 idempotent upsert
```

## Measured (local, single machine — see [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md) for methodology)

- **~5,000 orders/s** accepted sustained, **p99 7.2ms** ingest latency (60s ramp to 150 VUs, 400K+ orders per run)
- **Persistence keeps pace with ingest** after moving to batched consumption (`max-poll-records` + JDBC batch + `reWriteBatchedInserts`): a 420K-order burst was fully persisted by the time the load ended — v1's single-row inserts managed ~500/s and needed a 9.5-minute drain ([before/after](docs/FAILURE_MODES.md#batching-fix--beforeafter))
- **Zero loss / zero duplicates** across every run: `count(*) == count(DISTINCT id)` after full drain
- **Worker killed mid-burst** (62K-order run): consumer group rebalanced, in-flight messages redelivered, final counts exact — at-least-once delivery + `ON CONFLICT DO NOTHING` idempotency, no exactly-once machinery
- One distributed trace per order: `POST /orders` → `orders.v1 send` → `receive` → JDBC `query` (Tempo)

## Run it

```bash
docker compose up -d          # Redpanda, Postgres, Tempo, Prometheus, Grafana
docker exec order-platform-redpanda-1 rpk topic create orders.v1 --partitions 3
./gradlew :ingest-api:bootRun     # :8081
./gradlew :order-worker:bootRun   # :8082 (run twice with SERVER_PORT=8083 for a 2nd worker)

curl -X POST localhost:8081/orders -H "Content-Type: application/json" \
  -d '{"storeId":"waluigis","customer":{"name":"Ada"},"lines":[{"name":"Pie","quantity":1,"unitPriceCents":1799}],"totalCents":1799}'
```

Grafana at `localhost:3000` (Tempo + Prometheus pre-provisioned). Load test:

```bash
docker run --rm -i --add-host=host.docker.internal:host-gateway \
  -e BASE_URL=http://host.docker.internal:8081 \
  -v ./load:/scripts grafana/k6 run /scripts/orders.js
```

## Design

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — every layer and why, with diagram. [docs/DESIGN.md](docs/DESIGN.md) — milestone plan and decisions log. [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md) — chaos results, batching before/after, honest limits. Next: live dashboard (M4), then Azure Container Apps deploy for citable cloud numbers (M5).
