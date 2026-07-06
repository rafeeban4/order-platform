# Project log — order-platform

## 2026-07-05 — project start

- Purpose + full architecture locked in [DESIGN.md](DESIGN.md) — read that first.
- Goal owner context: hiring project for senior roles; public proof of resume's Kafka/scale claims; ties into pizza/taco demo business as "the real backend."
- Milestone M1 in progress: docker-compose (Redpanda + Postgres), Spring Boot ingest-api + order-worker skeletons.
- Java 21 + Gradle available on this machine (personal website backend uses same toolchain).

### Next steps (pick up here)

- [ ] docker-compose.yml: redpanda (v24+, single node) + postgres 16 + schema init SQL
- [ ] ingest-api: POST /orders → validate → ULID → KafkaTemplate publish `orders.v1` → 202
- [ ] order-worker: @KafkaListener group `order-workers`, idempotent upsert `ON CONFLICT DO NOTHING`
- [ ] Integration test: curl → poll Postgres row
- [ ] Then M2 (OTel) per DESIGN.md milestones

### Status end of 2026-07-05 session

- Skeleton compiles green (`gradlew build -x test`): ingest-api (POST /orders -> ULID -> Kafka 202) + order-worker (idempotent ON CONFLICT upsert). foojay resolver auto-provisions JDK 21 (machine has 25).
- ~~BLOCKER: Docker not installed~~ — resolved same day, owner installed Docker Desktop.

### 2026-07-05 (later) — M1 COMPLETE: pipeline verified end-to-end

- Docker engine 29.6.1. `docker compose up -d` → Redpanda + Postgres healthy; created `orders.v1` with 3 partitions (`rpk topic create orders.v1 --partitions 3` — note: topic creation is manual for now, consider auto-create or init container).
- Both services boot via `gradlew :ingest-api:bootRun` / `:order-worker:bootRun`.
- **E2E verified:** POST /orders → 202 `{"id":"01KWT4DV8ZBC4GGN787KD0B13B","status":"accepted"}` → row appeared in Postgres (`new`, 2997 cents) via the Kafka worker.
- **Burst test:** 200 sequential POSTs accepted, all 200 persisted, `count(*) == count(DISTINCT id)` — no loss, no dupes.
- Next: M2 per DESIGN.md — OpenTelemetry traces across ingest → broker → worker → DB, Prometheus metrics, consumer-lag visibility. Then M3 chaos (kill worker mid-burst) + k6.

### 2026-07-05 (night) — M2 COMPLETE: observability verified

- Deps: micrometer-tracing-bridge-otel + otlp exporter + prometheus registry (both services); datasource-micrometer (worker, JDBC spans). Kafka observation enabled via `spring.kafka.template.observation-enabled` (producer) and `spring.kafka.listener.observation-enabled` (consumer) — that's what links the trace across the broker.
- Compose additions: Tempo 2.6.1 (OTLP :4318/:4317, API :3200, `user: root` for local volume perms), Prometheus v2.55.1 (scrapes host services via `host.docker.internal` + `extra_hosts: host-gateway`), Grafana 11.4 (:3000, anonymous admin, both datasources provisioned).
- **Verified cross-service trace in Tempo:** single trace containing `http post /orders` → `orders.v1 send` (ingest-api) → `orders.v1 receive` → JDBC `connection`/`query` (order-worker). TraceQL used: `{ resource.service.name = "ingest-api" && name != "http get /actuator/prometheus" }`.
- **Verified Prometheus:** both targets `up`; `http_server_requests_seconds_*` histograms and `kafka_consumer_fetch_manager_*` metrics flowing.
- Sampling at 1.0 for dev — MUST drop (0.05–0.1) before k6 runs or tracing overhead pollutes the load numbers.
- Next: M3 — k6 scripts in `load/`, kill-worker-mid-burst chaos test, FAILURE_MODES.md, publish numbers.


### 2026-07-05 (late) - M3 COMPLETE: load + chaos, numbers published

- Baseline k6 (ramp to 150 VUs, 60s): 410,067 orders accepted (~5,250/s), p99 7.2ms, 0.03% request failures; ALL 410,067 persisted, zero dupes, ~9.5 min drain.
- Measured bottleneck: persistence ~400-650/s (single-row inserts) vs 5,250/s ingest. Next optimization: JDBC batch inserts in the worker.
- Chaos: 2 workers, 5 VUs x 60s (62,000 orders), worker 2 killed hard at t+20s -> group rebalanced to 1 member -> 62,000/62,000 rows, all distinct. Zero loss including in-flight redeliveries (idempotent upsert absorbed them).
- Trace sampling env-overridable (TRACE_SAMPLING), ran loads at 0.05.
- Full writeup: FAILURE_MODES.md (methodology + honest limits), README rewritten with measured numbers.
- k6 quirk: exits 255 through the PS pipeline even on passing thresholds - check output, not exit code.
- Remaining: broker-restart test (TODO in FAILURE_MODES), M4 dashboard, M5 Azure (citable cloud numbers), batch-insert optimization.

### 2026-07-06 - batching optimization, verified ~10x

- Worker moved to batch listener (batch=true on @KafkaListener, max-poll-records 500, fetch-min-size 64KB/fetch-max-wait 100ms) + jdbc.batchUpdate + reWriteBatchedInserts=true on the JDBC URL. Idempotency unchanged (per-row ON CONFLICT).
- Same k6 baseline rerun: 421,676 orders accepted (~4,774/s), ingest p95 improved slightly, and persistence KEPT PACE with ingest - zero backlog at first poll after load ended (v1 needed ~9.5 min drain at ~500/s). Zero loss/dupes again.
- Before/after table added to FAILURE_MODES.md; README updated. Note: batch listener means one consumer span per batch, not per record.
- Remaining: broker-restart test, M4 dashboard, M5 Azure.

### 2026-07-06 - broker-restart chaos + two real fixes

- Designing the test exposed fire-and-forget publish: 202 before broker ack = potential silent loss past delivery.timeout.ms. Fixed with send().get(5s) + 503 handler (bounded wait, honest failure).
- Ran it: 25s hard broker outage under 3-VU load. 12,273 202s, 15 timeouts (3 VUs x 5s cycles), instant recovery - but 12,288 rows: the 15 timed-out orders flushed from the producer buffer after recovery. Timeout = indeterminate, not failed.
- Fixed the retry hazard with Idempotency-Key header (key becomes order id -> ON CONFLICT absorbs retries). Verified: same key twice -> 1 row.
- FAILURE_MODES.md broker section written. Ingest restarted via Start-Process (background gradle task env got stopped) - kill stray java processes when done.
- Remaining: M4 dashboard, M5 Azure.
