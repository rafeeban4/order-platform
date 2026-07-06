# CLAUDE.md

Guidance for Claude Code sessions working in this repo.

## What this is

**order-platform** — an event-driven order-processing pipeline built as
*hiring proof* for senior backend roles, with **published, reproducible load +
chaos numbers**. Spring Boot ingest → Redpanda/Kafka (`orders.v1`, 3
partitions) → consumer-group workers → Postgres, traced end-to-end with
OpenTelemetry (Tempo + Prometheus + Grafana).

```
k6 ──► ingest-api ──► orders.v1 ──► order-worker ×N ──► Postgres
       (202 + ULID)    Redpanda      idempotent upsert
```

- **Stack:** Java 21 · Spring Boot · Redpanda/Kafka · Postgres · OpenTelemetry
  · Gradle multi-module (`ingest-api`, `order-worker`) · Docker Compose · k6.
- **Repo is PUBLIC** (`https://github.com/rafeeban4/order-platform`) — **never
  commit anything secret-ish** (keys, tokens, real endpoints, PII).

## Where the real documentation lives

Read in this order before changing anything:

1. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — **read first**; the design.
2. [docs/PROJECT_LOG.md](docs/PROJECT_LOG.md) — session history; each entry
   ends with gotchas at the bottom. Read before continuing work.
3. [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md) — the measured numbers +
   methodology (throughput, batching before/after, chaos/kill test).
4. [docs/DESIGN.md](docs/DESIGN.md) — deeper design notes.
5. [README.md](README.md) — overview + the run commands + headline numbers.

Supporting: [docs/RESUME_BULLETS.md](docs/RESUME_BULLETS.md) (résumé phrasing),
[docs/BLOG_DRAFT.md](docs/BLOG_DRAFT.md) (**dropped — do not publish**).

## How to run

```bash
docker compose up -d          # Redpanda, Postgres, Tempo, Prometheus, Grafana
docker exec order-platform-redpanda-1 rpk topic create orders.v1 --partitions 3   # once
./gradlew :ingest-api:bootRun     # :8081
./gradlew :order-worker:bootRun   # :8082 (run again with SERVER_PORT=8083 for a 2nd worker)
```

Grafana at `localhost:3000` (Tempo + Prometheus pre-provisioned). Load test
lives in `load/` (k6).

## Gotchas / conventions

- **k6 and Gradle exit non-zero through PowerShell even on success** — read the
  actual output, not the exit code, before concluding something failed.
- Verify with `./gradlew build` before committing.
- **Azure / any billable cloud deploy is out of scope permanently.** The
  numbers are local-single-machine and stay that way; don't stand up paid
  compute. (A free static *replay* demo lives separately in `demo-site/` on
  Firebase Hosting — hosting only, no live pipeline.)
- Commits end with `Co-Authored-By: Claude <model> <noreply@anthropic.com>`.
