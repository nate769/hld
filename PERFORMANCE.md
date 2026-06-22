# Performance Report

Covers suggestion latency (including p95), cache hit rate, and the write
reduction from batching. Every number below is reproducible against a running
cluster with the included harness, no synthetic figures.

```bash
docker compose up --build      # bring the cluster up and seed
bun run bench                  # latency + hit rate + write reduction
bun run demo:recency           # basic-vs-recency ranking demo
```

## Measurement setup

- Topology: the full `docker-compose` cluster, 1 LB to 3 app nodes to 3 Redis
  shards plus Postgres and the cache-updater. All requests go through the
  public LB (`http://localhost:8080`), so figures include the consistent-hash
  proxy hop, not just a raw Redis read.
- Host: single developer machine, all containers local. Latency is therefore
  optimistic: there's no real network between the LB and the app nodes. Treat
  the absolute numbers as a lower bound; what generalises is the shape (cache
  vs DB-fallback, batched vs per-request writes).
- Dataset: 93,387 unique queries / 1.72M search events (see
  [Dataset](./README.md#dataset)).
- Tooling: `scripts/bench.ts` uses only the public HTTP API and `/metrics`.
  4,000 `/suggest` requests at concurrency 32 after a 200-request warm-up.

## 1. Suggestion latency (`GET /suggest`)

4,000 requests over ~12.5k distinct dataset prefixes, concurrency 32:

| Ranking             | mean   | p50    | p90    | p95        | p99    | max    | throughput    |
| ------------------- | ------ | ------ | ------ | ---------- | ------ | ------ | ------------- |
| `basic` (count)     | 0.63ms | 0.52ms | 1.05ms | **1.21ms** | 2.26ms | 2.77ms | ~50,400 req/s |
| `recency` (blended) | 0.63ms | 0.52ms | 1.12ms | **1.35ms** | 2.49ms | 3.31ms | ~50,800 req/s |

Why recency is just as fast: both rankings are a single `ZREVRANGE` over a
pre-computed per-prefix ZSET (`q:<prefix>` vs `qr:<prefix>`). The blend is
computed at write/rebuild time by the cache-updater, never on the read path,
so the recency ranking adds no read latency.

## 2. Cache hit rate (`/suggest` cache vs Postgres fallback)

`/suggest` reads the Redis shard first and falls back to Postgres on a miss,
then warms the cache (marks the prefix dirty). `/metrics` counts both.

- Over the benchmark run: 4,000 of 4,000 = 100% hit rate on seeded prefixes.
- The fallback path is real and was verified by evicting a live key:

  ```text
  /suggest?q=goo            -> source: cache         # served from redis3
  redis3> DEL q:goo                                  # evict the hot key
  /suggest?q=goo            -> source: db            # Postgres fallback
  ...cache-updater rebuilds...
  /suggest?q=goo            -> source: cache         # rewarmed automatically
  ```

A 100% steady-state hit rate is expected because the seeder pre-derives every
prefix's top-K. Misses happen for never-before-seen prefixes from live searches
(warmed on first request) and for genuinely empty prefixes. The `source` field
on every `/suggest` response makes hit vs miss directly observable.

## 3. Write reduction from batching

App nodes buffer `POST /search` in memory and flush to Postgres in batches
(`BATCH_SIZE=100`, or after `FLUSH_INTERVAL_MS`). Measured via `/metrics`:

| Metric                          | Value     |
| ------------------------------- | --------- |
| Searches submitted              | 2,000     |
| Postgres batch **transactions** | 22        |
| **Write reduction**             | **90.9x** |
| Unique rows upserted            | 410       |

So ~91 search requests collapsed into a single database transaction. Without
batching, those 2,000 searches would be 2,000 synchronous writes (plus per-row
index maintenance); batching turns them into 22 multi-row upserts. The
cumulative cluster figure across the whole session was 83.3x
(`searches=2,417 / batches=29`).

Knobs: a larger `BATCH_SIZE` gives a higher reduction but more data at risk on
crash; a shorter `FLUSH_INTERVAL_MS` gives fresher data but smaller batches.
See the [failure trade-offs](./README.md#batch-writes-and-failure-trade-offs).

## 4. Distribution across shards (consistent hashing)

The seeder routed 1,352,775 derived top-K rows across the three shards by
`hash(prefix)`:

| Shard | Cache rows (top-K members) | Redis keys (`q:` + `qr:` + trending) |
| ----- | -------------------------- | ------------------------------------ |
| 1     | 431,490                    | 602,437                              |
| 2     | 334,549                    | 470,035                              |
| 3     | 586,736                    | 793,371                              |

The spread is uneven because the prefix keyspace itself is skewed (English
queries clump on common leading characters), not because the ring is
unbalanced. The ring distributes random keys to within ±25% (see `bun test`).
Example ownership (`GET /cache/debug?prefix=...`): `google` -> app1, `yahoo` ->
app2, `facebook` -> app3.

## Reproducing

```bash
bun run bench                                   # table 1-3
BENCH_RANK=recency bun run bench                # recency-rank latency
bun run demo:recency                            # reorder before/after
curl -s localhost:8080/metrics | jq             # live cluster counters
docker compose exec redis3 redis-cli DEL q:goo  # force a cache miss to see fallback
```
