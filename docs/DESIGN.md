# Order Platform — event-driven order processing, publicly load-tested

**Purpose (why this exists):** Rafee's resume claims distributed-systems scale (Kafka 15K msg/s, 250K orders/day, 1.5M txn/mo) that lives behind employer walls. This project is the public, verifiable version: an event-driven order pipeline with published load-test numbers, tracing, and failure-mode documentation. It doubles as the "real backend" story for the Waluigi's/Taco Tornado demo business (waluigis-demo.web.app, taco-tornado-demo.web.app).

**Audience:** senior-engineer interview loops at good companies. Every design choice should generate an interview talking point.

## Architecture (v1)

```
k6 / demo sites                    ┌─────────────┐
      │  POST /orders              │  dashboard  │  (React, live feed)
      ▼                            └──────▲──────┘
┌──────────────┐    orders.v1            │ WebSocket (STOMP)
│  ingest-api  │──► Redpanda/Kafka ──►┌──┴───────────┐
│ (Spring Boot)│    (3 partitions)    │ order-worker │──► Postgres
└──────────────┘                      │ (consumer    │    (orders, outbox)
   validates, assigns id,             │  group, N=2+)│
   returns 202 + Location             └──────────────┘
```

- **ingest-api** — Spring Boot 3 / Java 21. Validates order JSON, assigns ULID, publishes to `orders.v1`, returns 202 immediately. No DB write on the hot path — that's the whole point (talking point: why 202 + async vs sync insert).
- **Kafka** — Redpanda locally (single binary, no ZooKeeper) and in cloud (cheap). Topic `orders.v1`, keyed by store id → per-store ordering. Status changes on `order-status.v1`.
- **order-worker** — consumer group; idempotent upsert into Postgres (`ON CONFLICT (id) DO NOTHING` — talking point: at-least-once delivery + idempotency instead of exactly-once). Emits status events. Kill a worker mid-batch → rebalance → no lost/duplicated orders (documented failure test).
- **dashboard** — the kitchen-screen story at scale: live order feed over WebSocket, counters (orders/sec, p99 ingest latency, consumer lag).
- **Observability** — OpenTelemetry traces (ingest → broker → worker → DB) exported to Grafana/Tempo locally; Application Insights on Azure (cert tie-in). Prometheus metrics: ingest latency histogram, consumer lag, DB write throughput.
- **Load test** — k6 scripts in repo; publish results table in README: orders/min sustained, p50/p99 latency, on exactly-priced infra ("$X/mo Azure Container Apps"). Honest methodology section.

## Milestones

1. **M1 — local pipeline:** docker-compose (redpanda, postgres, grafana+tempo+prometheus optional later), ingest-api, order-worker, idempotent persistence, integration test proving order flows end-to-end. *Definition of done: `docker compose up` + one curl → row in Postgres.*
2. **M2 — observability:** OTel tracing across the pipeline, metrics, consumer-lag visibility.
3. **M3 — load + chaos:** k6 scenarios (ramp, spike, soak-lite), worker-kill test, broker-restart test. Write FAILURE_MODES.md with observed behavior.
4. **M4 — dashboard:** live WebSocket feed + counters (small React app, reuse demo-site design system).
5. **M5 — Azure:** Container Apps (ingest, worker), Azure Database for PostgreSQL flexible (burstable) or Neon, Kafka = Redpanda serverless free tier or Container App. Cost note per component. CI via GitHub Actions.
6. **M6 — writeup:** README with architecture diagram, published numbers, trade-offs; link from portfolio Projects page.

## Decisions log

- Java 21 + Spring Boot 3: matches resume + target interviews (over Node/Go).
- Redpanda over Apache Kafka image: single-container, fast local start; wire-compatible so all Kafka talking points hold.
- At-least-once + idempotent consumer over exactly-once semantics: simpler, honest, and the standard senior answer.
- ULID order ids: sortable, generated at ingest without DB round-trip.
- No auth in v1 (public demo API is rate-limited at ingress instead) — revisit at M5.

## Repo layout (planned)

```
order-platform/
  docker-compose.yml
  ingest-api/      (Spring Boot, Gradle)
  order-worker/    (Spring Boot, Gradle — separate deployable)
  dashboard/       (Vite React, M4)
  load/            (k6 scripts)
  docs/            (DESIGN.md, FAILURE_MODES.md, PROJECT_LOG.md)
```
