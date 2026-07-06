# Failure modes — observed behavior, not theory

All results from real runs on 2026-07-05, single Windows 11 dev machine (services on host JVMs, infra in Docker). Methodology: k6 in Docker → ingest-api → Redpanda (3 partitions) → 2 order-worker instances (one consumer group) → Postgres 16.

## Baseline load (no faults)

k6 ramp 0→150 VUs over 60s:

| Metric | Result |
|---|---|
| Orders accepted | **410,067** in ~78s (~5,250/s sustained) |
| Ingest latency | p50 2.3ms · p95 5.7ms · **p99 7.2ms** · max 448ms |
| Failed requests | 0.03% (153 connection-level, during peak ramp) |
| Persisted | **410,067 rows — count(*) == count(DISTINCT id)**, zero loss, zero duplicates |

**Honest asymmetry (v1, single-row inserts):** ingest accepted ~5,250/s but the workers persisted ~400–650/s, so a 60s burst took ~9.5 minutes to drain. That's the queue doing exactly its job — absorbing a spike the database can't take in real time — and it was the #1 optimization target.

## Batching fix — before/after

Change: batch `@KafkaListener` (up to `max-poll-records: 500` per poll) + one `jdbc.batchUpdate` per batch + `reWriteBatchedInserts=true` on the JDBC URL (the Postgres driver flattens the batch into multi-row INSERTs). Idempotency unchanged — every row is still `ON CONFLICT DO NOTHING`.

Same k6 scenario rerun:

| | v1 single-row | v2 batched |
|---|---|---|
| Orders accepted | 410,067 (~5,250/s) | 421,676 (~4,774/s) |
| Ingest p95 | 5.7ms | 5.0ms |
| Drain time after load ended | **~9.5 minutes** | **0 — already drained at first poll** |
| Effective persistence rate | ~400–650 rows/s | keeps pace with ingest (≥4,800 rows/s) |
| Loss / duplicates | 0 / 0 | 0 / 0 |

~10× persistence throughput from ~30 lines of change, with the correctness properties intact. The lesson worth telling: the WAL-sync and network round-trip per statement was the cost, not Postgres itself.

## Worker killed mid-burst

Setup: 2 worker instances, constant 5 VUs for 60s (~1,000 orders/s). At t+20s, worker 2's process was killed hard (no graceful shutdown).

Observed:

1. Consumer group rebalanced: 3 partitions reassigned to the surviving worker (group state returned to Stable, 1 member).
2. Ingest was unaffected — 62,000 orders accepted, 0.01% request failures (unrelated to the kill; ingest never touches workers).
3. Final state: **62,000 accepted → 62,000 rows, all distinct.** Zero orders lost, zero duplicated, including any in-flight messages the dead worker had received but not committed — those were redelivered to the survivor and the `ON CONFLICT DO NOTHING` upsert made redelivery a no-op.

Why this works (the design, not luck): offsets are committed after processing (at-least-once), so a crash can only cause *redelivery*, never loss; idempotent writes make redelivery harmless. No exactly-once machinery needed.

## Broker restart — not yet run

Planned: restart Redpanda mid-burst; expect ingest 5xx during outage (producer acks=all fails fast), full recovery after; orders accepted before/after unaffected. TODO in a future session.

## Known limits of these numbers

- One machine: k6, JVMs, and containers share CPU — real-network latency and cross-host throughput will differ. Azure Container Apps numbers (M5) will be the citable ones.
- Trace sampling was 5% during load runs (100% would skew latency).
- Postgres is untuned (default `postgres:16-alpine`); persistence rate reflects unbatched inserts, not Postgres limits.
