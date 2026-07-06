# Blog draft — "My chaos test caught my API lying"

> Draft for dev.to / LinkedIn article / personal site. ~6 min read. Publishing this is optional but high-leverage: it's the artifact recruiters and interviewers actually click. Edit voice to taste before posting.

---

## My chaos test caught my API lying about durability

I built an order-processing pipeline to prove out some distributed-systems claims in public — Spring Boot ingest, Kafka (Redpanda), a consumer group writing to Postgres. Load tests went great: ~5,000 orders/sec sustained, p99 of 7ms, zero loss across 400K-order runs.

Then I designed the broker-restart test, and before I even ran it, the design review caught bug #1.

### Bug 1: the 202 that wasn't true

My ingest endpoint did this:

```java
kafka.send("orders.v1", storeId, payload);   // fire and forget
return ResponseEntity.accepted()...          // "got it!"
```

`KafkaTemplate.send()` returns a future. I wasn't awaiting it. With the broker healthy, no problem — the record lands milliseconds later. But during a broker outage, records queue in the *client-side* producer buffer, and my API keeps cheerfully returning 202. If the outage outlasts `delivery.timeout.ms` (default 2 minutes), those buffered records are dropped — orders my API had *accepted* would silently vanish.

A 202 is a promise. Mine was a lie with a 2-minute fuse.

Fix: await the `acks=all` confirmation with a bounded timeout, and turn "can't confirm durability" into an honest 503:

```java
kafka.send("orders.v1", storeId, payload).get(5, SECONDS);
```

### Running the test: 25 seconds of darkness

Setup: constant load, then `docker stop` the broker at t+20s, restart at t+45s.

Result: 12,273 orders got a 202. During the outage, exactly 15 requests failed — 3 virtual users × 5-second timeout cycles. Throughput *paused* instead of lying. Recovery was instant when the broker came back.

Then I counted the rows: **12,288**.

### Bug 2: fifteen orders that shouldn't exist

Fifteen more rows than 202s. The requests that timed out — the ones my API told the client had *failed* — had their records sitting in the producer buffer, and when the broker returned, those records flushed and got persisted.

The client heard "failed." The kitchen got the order anyway.

This is the part that generalizes: **a timeout is not a failure — it's an unknown.** The work might have happened. You cannot make the unknown go away with any amount of engineering; the send is in flight somewhere you can't see. All you can do is make *retries safe*.

Which is why every serious payment and ordering API (Stripe's is the canonical example) supports idempotency keys:

```
POST /orders
Idempotency-Key: a1b2c3d4-retry
```

I made the client's key *become* the order's primary key. A retry after a timeout lands on the same row, and the existing `ON CONFLICT DO NOTHING` — originally there to absorb Kafka's at-least-once redeliveries — absorbs the client's retry too. One mechanism, both duplicate sources. Verified: same key POSTed twice → same id returned twice → exactly one row.

### What I'd tell you to take away

1. **Fire-and-forget + a success status code is a durability bug**, and it's invisible until the dependency behind you goes down. Await the ack or don't claim success.
2. **Chaos tests find bugs at design time too.** Bug 1 fell out of just *writing down* what the test should observe.
3. **Idempotency is one design decision that pays three times**: broker redeliveries, consumer crashes, and client retries all collapse into the same harmless no-op.

Everything here is reproducible — the repo has the k6 scenarios, the failure-mode writeups with real numbers, and the commits where each fix landed: **github.com/rafeeban4/order-platform**

---

*Suggested tags: distributed-systems, kafka, java, chaos-engineering, backend*
