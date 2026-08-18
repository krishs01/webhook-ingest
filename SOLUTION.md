# SOLUTION

## What was broken, and why

**1. Race condition in `Cache.Record()` — stats drift upward.**
The `Record()` method read and wrote the in-memory map without holding the mutex, even though `Get()` properly used `RLock`. Under concurrent webhook requests this caused data races: goroutines would read stale counters, increment them, and write back, silently losing updates or double-counting. This explained the "call-counts drifting higher than the actual number of calls" symptom.

**2. `processRecording` used the request-scoped context — recordings never marked processed.**
The recording goroutine received `ctx` from the HTTP handler. As soon as the handler returned `200 OK`, that context was cancelled. The `MarkRecordingProcessed` database call inside the goroutine then failed with a context-cancelled error — but the error was swallowed by a `// TODO: handle` comment, so nothing appeared in the logs.

**3. Fire-and-forget goroutines lost on deploy — in-flight work disappears.**
Recording goroutines were launched with bare `go func()` and never tracked. `http.Server.Shutdown()` waits for active HTTP handlers to finish, but not for goroutines those handlers spawned. On SIGTERM the process exited while goroutines were still sleeping, silently discarding their work.

**4. No UNIQUE constraint on `events.event_id` — duplicate events and inflated stats.**
The `events` table had a plain B-tree index on `event_id`, not a UNIQUE index. The deduplication logic used a check-then-insert pattern (`EventExists` → `InsertEvent`), which is a classic TOCTOU race: two concurrent deliveries of the same event could both pass the existence check, both insert, and both increment `account_stats`. This was the root cause of duplicate call records in the dashboard.

## Why I chose this deduplication strategy

I used a Postgres `UNIQUE` index on `events.event_id` combined with `INSERT ... ON CONFLICT (event_id) DO NOTHING`, checking `RowsAffected()` to decide whether to proceed with side effects.

**Why Postgres over Redis `SETNX`:**
- The event row must be written to Postgres regardless, so making the dedup check part of the same `INSERT` turns it into a single atomic operation with zero TOCTOU window.
- A Redis-based approach (e.g., `SETNX` with a TTL) would add a second system that must agree on dedup state. If Redis goes down or the key expires before the Postgres insert, duplicates slip through. If the Postgres insert fails after Redis accepts the key, the event is lost.
- The UNIQUE index is durable, requires no TTL tuning, and survives restarts. It is the simplest correct solution.

**Trade-off acknowledged:** This approach means every insert does a uniqueness check against the index, which adds a small amount of I/O. For the current throughput this is negligible.

**Verified under real contention, not just sequential duplicates:** `TestConcurrentDuplicateDelivery` fires 20 goroutines that all POST the same `event_id` at the same instant (synchronised via a channel, not just a loop). After all 20 requests return `200 OK`, exactly one row exists in `events` and `account_stats.call_count == 1`. This is the test that would have failed against the old check-then-insert (`EventExists` → `InsertEvent`) pattern, since that TOCTOU window only opens under genuine concurrency — a sequential test can't expose it.

## What I would change at 10,000 webhooks/second

- **Batch inserts with a write buffer.** Instead of one `INSERT` per request, buffer incoming events in memory (or a Redis list) and flush batches of 100–500 rows using `COPY` or multi-row `INSERT ... ON CONFLICT`. This amortises round-trip and WAL costs.
- **Partition the `events` table** by time (e.g., daily) so the UNIQUE index stays small and hot, and old partitions can be detached cheaply.
- **Move recording processing to a durable queue** (e.g., Redis Streams, SQS, or a Postgres-backed job table) with consumer workers, instead of in-process goroutines. This decouples ingestion throughput from recording processing latency and survives deploys without a WaitGroup.
- **Connection pooling with PgBouncer** in front of Postgres to handle the connection fan-out from many concurrent requests without exhausting `max_connections`.
- **Redis-based fast-path dedup** (`SETNX` with a TTL of a few hours) to reject obvious duplicates before they hit Postgres, keeping the UNIQUE index as the durability backstop.
