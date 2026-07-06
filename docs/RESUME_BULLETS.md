# Resume material — order-platform

Ready-to-paste options for the SOFTWARE PROJECTS section (pick 3–4; every number is reproducible from the repo). Also works as the LinkedIn featured-project blurb.

**Header line:**
> **Order Platform** — event-driven order pipeline, public repo: github.com/rafeeban4/order-platform · *2026*

**Bullets (choose by emphasis):**

- Built an event-driven order pipeline (Java 21, Spring Boot, Kafka/Redpanda, Postgres) sustaining **~5,000 orders/sec at 7ms p99** ingest latency, with **zero orders lost or duplicated** across 400K+ order load tests (k6, published methodology)
- Chaos-tested reliability: killing a consumer mid-burst triggered a clean rebalance with **62,000/62,000 orders persisted exactly** via at-least-once delivery + idempotent `ON CONFLICT` writes
- Chaos testing surfaced two production-class bugs — a durability gap (202 before broker ack) and retry indeterminacy after timeouts — fixed with awaited `acks=all` publishing and **Stripe-style Idempotency-Key** support
- Eliminated a measured 10× persistence bottleneck (~500 → ~5,000 rows/sec) with batched Kafka consumption + JDBC batch rewriting, re-verified against the identical load scenario
- Instrumented end-to-end **OpenTelemetry tracing** across the async boundary (HTTP → Kafka → JDBC in one trace), Prometheus metrics, and a live WebSocket ops dashboard that broadcasts only committed data

**Interview one-liner:**
> "My resume says I've done Kafka at scale — this repo is me proving it where you can check: load numbers, kill tests, and the two bugs the chaos tests caught, all written up."

**Where it fits on the current resume:** replaces or sits above SCENIC in SOFTWARE PROJECTS (SCENIC is 2021 and weaker signal). Consider retitling the section "Selected Projects."
