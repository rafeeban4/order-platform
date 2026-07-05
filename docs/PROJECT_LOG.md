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
- **BLOCKER for E2E: Docker is not installed** � docker-compose.yml (Redpanda + Postgres) can't run. Install Docker Desktop (WSL2 backend), then: `docker compose up -d` then run both apps and `curl -X POST localhost:8081/orders` with a JSON body, verify row in Postgres (port 5433, orders/orders_local_dev).

