# Distributed Search Typeahead

A sharded autocomplete service. Postgres is the durable source of truth for
query counts; the three Redis shards hold a derived per-prefix top-K cache that
a consistent-hash load balancer reads from. A central cache-updater keeps the
shards in sync from Postgres, so a live search becomes visible in suggestions
on any shard while each app node still talks only to its own Redis. Built on
Bun (`Bun.serve`, `Bun.RedisClient`, `Bun.sql`), with no Express, no `ioredis`,
no `pg`.

![Architecture](./Architecture.png)

Reads (`/suggest`) and writes (`/search`) enter via the consistent-hash load
balancer. App nodes batch-write counts to central Postgres; the updater polls
Postgres and pushes refreshed top-K caches through the app nodes, so each app
node remains the sole writer of its own Redis.

## What's where

| Area                         | Where it lives                                                                                                  |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Basic implementation         | dataset ingestion in `scripts/seed.ts`; suggestions UI in `public/`; `GET /suggest` + `POST /search`; query-count store in Postgres; distributed cache across 3 Redis shards via `src/hash-ring.ts`; cache miss falls back to Postgres. |
| Trending searches            | recency-aware `/suggest?rank=recency` plus `recent_count` decay (see [recency ranking](#trending--recency-aware-ranking)); global trending board on `GET /trending`. Demo: `bun run demo:recency`. |
| Batch writes                 | in-memory buffer with a `BATCH_SIZE`/interval flush in `src/server.ts`; measured ~91x write reduction ([PERFORMANCE.md](./PERFORMANCE.md#3-write-reduction-from-batching)); failure trade-offs covered below. |
| Deliverables                 | this README, [API docs](./docs/API.md), [performance report](./PERFORMANCE.md), `Architecture.png`, [screenshots](#screenshots), [dataset](#dataset), design notes below. |

Endpoints, all behind the LB: `GET /suggest?q=&rank=basic|recency`, `POST /search`,
`GET /trending`, `GET /cache/debug?prefix=`, `GET /metrics`. Full reference in
[`docs/API.md`](./docs/API.md).

## How it works

| Concern              | Where                                  | What                                                                                                                                                                                                                                         |
| -------------------- | -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Consistent hashing   | `src/hash-ring.ts`                     | FNV-1a with 150 virtual nodes. `route(key)` maps a prefix or query to a shard. The seeder, LB, app nodes and updater all import it. |
| Source of truth      | `src/db.ts` + Postgres                 | `query_counts(query, count)` holds the authoritative totals. `dirty_prefixes(prefix, dirty_at)` is the work queue for the cache. |
| Search               | `src/server.ts` `POST /search`         | Push the query into the in-memory buffer, return `202` right away. |
| Batch writer         | `src/server.ts`                        | When the buffer hits `BATCH_SIZE` (100), or after `FLUSH_INTERVAL_MS`, dedup the counts, upsert into Postgres and mark each prefix dirty in one transaction, then `ZINCRBY` the local trending ZSET. |
| Cache updater        | `src/cache-updater.ts`                 | Polls `dirty_prefixes`, claims them with `DELETE ... RETURNING`, recomputes each prefix's top-K from Postgres, groups by `route(prefix)`, and POSTs each batch to that shard's app node (`/internal/cache`). Holds no Redis connection. |
| Internal cache write | `src/server.ts` `POST /internal/cache` | The app node applies the pushed top-K to its own Redis via an atomic Lua `DEL`+`ZADD`, rejecting any prefix it doesn't own. So each shard has exactly one writer. Not proxied by the LB. |
| Suggest              | `src/server.ts` `GET /suggest`         | `ZREVRANGE` on the local shard, up to 10 suggestions. `rank=basic` reads `q:<prefix>` (all-time count); `rank=recency` reads `qr:<prefix>` (blended score). On a cache miss it falls back to Postgres (`source:"db"`) and warms the cache. |
| Recency ranking      | `src/db.ts` + cache-updater            | A second per-prefix cache `qr:<prefix>` ordered by `log2(1+count) + 3*log2(1+recent_count)`. `recent_count` is bumped per search and decayed, so active queries can outrank stale giants. Same `/suggest` API, two orderings. |
| Cache debug          | `GET /cache/debug?prefix=<p>`          | LB hashes the prefix, proxies to the owning node, which reports `{ node, shard, status: hit/miss, cached, recencyCached }`. Useful to see which node owns a prefix and whether it's cached there. |
| Metrics              | LB `GET /metrics`                      | Fans out to all nodes and sums counters: cache hit rate, searches-per-batch write reduction, rows upserted. Backs the [performance report](./PERFORMANCE.md). |
| Trending             | LB `GET /trending`                     | Each shard keeps a local `trending` ZSET; the LB fans out to all nodes and merges. Each query lives on exactly one shard. |
| Time decay           | `src/server.ts` / `cache-updater.ts`   | Trending: every `DECAY_INTERVAL_MS` (24h) a Lua script multiplies trending scores by `DECAY_FACTOR` (0.9). Recency: the updater decays `recent_count` every `RECENCY_DECAY_INTERVAL_MS` and re-marks affected prefixes so the served order fades. |
| Load balancer        | `src/lb.ts`                            | Hashes `/suggest`/`/search` with the same ring, proxies to the owning node, logs each routing decision, serves the frontend. |
| Seeding              | `scripts/seed.ts`                      | Two phase: load `query_counts` from the dataset, then derive the shard caches from SQL top-K per prefix (the same policy the updater uses live). |
| Frontend             | `public/`                              | Vanilla JS, 150ms debounced suggestions, plus a "Trending right now" board. |

### Data model

- Postgres `query_counts(query PK, count BIGINT, recent_count BIGINT)` holds
  the durable totals plus a decaying recency signal. `dirty_prefixes(prefix
  PK, dirty_at)` is the cache work queue.
- Redis per shard: `q:<prefix>` is a ZSET of the top-`CACHE_K` queries by
  all-time count. `qr:<prefix>` is the same prefix ranked by the blended
  recency score. `trending` is a per-shard ZSET, decayed daily.

## How the write path stays consistent across shards

The earlier design wrote the suggestion cache directly from the app node, which
meant a live `/search` for query `q` only updated prefixes on `hash(q)`'s shard.
Prefixes owned by other shards never saw it (the read for `/suggest?q=p`
happens on `hash(p)`'s shard). Strict 1:1 networking made cross-shard writes
impossible.

The fix: Postgres is the single source of truth, and the cache-updater
computes each prefix's top-K and pushes it, through the owning shard's app
node, into that shard's cache by `route(prefix)`. App nodes write counts to
Postgres and own their Redis; the updater holds no Redis connection and reaches
shards only via the app nodes' `POST /internal/cache`. So each Redis shard has
exactly one writer (its app node), yet every live search becomes globally
visible, because the cache is updated on the same shard `/suggest` reads from.

Verified end to end: searching a query whose short prefix routes to a
different shard than the full query (e.g. `"plmk demo"`: query goes to shard 2,
prefix `"plmk"` goes to shard 3) appears in `/suggest?q=plmk` (served by shard
3). Since the updater has no Redis access, the cache could only have reached
redis3 through app3.

## Dataset

`data/search_frequencies.json` is an array of `{ "query", "count" }` objects:
an aggregated search-query frequency log. Counts are real aggregate
frequencies (the rawest row is the blank `"-"` search, then `google`, `yahoo`,
and so on), so popularity ranking is meaningful out of the box.

> Note: the exact upstream source isn't recorded in this repo's history. The
> shape resembles a classic web search-query log, but cite the concrete source
> before submission.

| Metric                              | Value             |
| ----------------------------------- | ----------------- |
| Raw rows                            | 93,396            |
| Unique queries (after junk filter)  | 93,387            |
| Total search events (sum of counts) | 1,724,222         |
| Top query                           | `google` (32,396) |

On the "100,000 queries" minimum: the file has 93,387 unique queries (a touch
under the 100k row line) but 1.72M total search events (well over 100k by
volume). The loader filters 9 pure-punctuation junk rows, most importantly the
`"-"` blank-search row with ~98k hits that would otherwise dominate the `q:`
and `q:-` caches with noise. Real single-letter queries like `g` are kept. To
swap in a larger dataset, drop any `[{query,count}]` JSON at
`data/search_frequencies.json` and re-seed.

Loading: `docker compose up` runs the one-shot `seed` job automatically
(`scripts/seed.ts`). It loads `query_counts` into Postgres, then derives every
prefix's top-K and bulk-loads both shard caches (`q:` and `qr:`) via pipelined
`ZADD`. For local dev: `bun run seed`.

## Trending and recency-aware ranking

`/suggest` supports two orderings over the same candidate set, behind the same
API:

- `rank=basic` (default) sorts by all-time `count`.
- `rank=recency` sorts by a blended score so recently-active queries can
  outrank stale historical giants.

```
score = HIST_WEIGHT * log2(1 + count) + RECENCY_WEIGHT * log2(1 + recent_count)
        (defaults: HIST_WEIGHT=1, RECENCY_WEIGHT=3)
```

Design notes:

1. Recent searches are tracked by bumping both `count` (permanent) and
   `recent_count` (recent activity) on the query's Postgres row when a batch
   flushes. One extra column, no new store.
2. Recent activity affects ranking via the blended score with weight
   `RECENCY_WEIGHT` (default 3x). Both signals are log2-compressed (diminishing
   returns: the millionth search matters less than the first), so a burst on a
   modestly-popular query can overtake an all-time leader, while a single
   search can't (no flapping).
3. Short-lived spikes don't rank forever because the cache-updater decays
   `recent_count` (x0.5 each interval) and re-marks the affected prefixes
   dirty, so the served recency cache actually fades. `count` is never decayed,
   so true long-term popularity persists. A spike that goes quiet converges
   back to its all-time rank.
4. The cache reflects ranking changes because the updater recomputes the
   blended top-K from Postgres whenever a prefix is dirtied (per search and per
   decay tick) and pushes the refreshed `qr:<prefix>` ZSET to the owning shard.
   Reads stay an O(1) `ZREVRANGE`.
5. Trade-offs: derive-time blending keeps `/suggest` reads fast (recency adds
   no read latency, see [PERFORMANCE.md
   §1](./PERFORMANCE.md#1-suggestion-latency-get-suggest)). The cost is a
   second cache (roughly 2x suggestion-cache memory) and eventual consistency:
   recency updates lag a search by one updater cycle.

> Concurrency note: the decay (`UPDATE ... recent_count = floor(recent_count*f)`)
> and the per-search increment (`UPSERT ... recent_count + delta`) are both
> single-row atomic `UPDATE`s, so Postgres row-locking serialises them. A
> decay and an increment on the same query can't interleave or lose a write;
> the only effect is ordering, which is harmless for an approximate recency
> signal.

The spike rises (`demo:recency`):

```bash
bun run demo:recency      # bursts a query, prints basic vs recency side by side
```

```
AFTER  -- go surge demo is #4 by count, #1 by recency
  #   rank=basic (count)              rank=recency (blended)
   1    google                        > go surge demo
   4  > go surge demo                   google maps
```

And the spike fades (verified end to end). With the updater decaying every 8s,
a 300-search burst on `go fade test` was watched in `/suggest?q=go&rank=recency`:

```
risen:  #1  ->  #1 ... #3 ... #7   (recent_count: 300 -> ... -> 0)
final:  basic(count) rank = #7   |   recency rank = #7      # converged exactly
        Postgres row: count=300 (kept), recent_count=0 (decayed away)
```

The boosted query decayed back to its true all-time rank (#7), not out of the
list, which is the proof it can't be permanently over-ranked. Its permanent
`count` is untouched.

A globally merged trending board (`GET /trending`) is also rendered on the UI,
backed by per-shard `trending` ZSETs with a daily 10% decay.

## Batch writes and failure trade-offs

`POST /search` never writes synchronously. The query is pushed into an
in-memory buffer and `202` is returned immediately. A flush is triggered the
moment the buffer hits `BATCH_SIZE` (100), with a timed `FLUSH_INTERVAL_MS`
safety net for low traffic. A flush dedups counts in memory (via a Map), then
upserts them into Postgres and marks affected prefixes dirty in one
transaction.

Measured: ~91 searches per database transaction (90.9x write reduction). See
[PERFORMANCE.md §3](./PERFORMANCE.md#3-write-reduction-from-batching).

Failure trade-offs:

- A crash before flush loses the buffered window. Up to `BATCH_SIZE-1`
  searches (or a `FLUSH_INTERVAL_MS` worth) live only in memory; a hard crash
  drops them. We accept this because the data is approximate popularity
  counts, not orders. Losing a few increments out of millions never changes a
  top-K. The window is bounded and tunable.
- Mitigations: `SIGTERM`/`SIGINT` flush the buffer before exit, and a failed
  batch is re-queued to the front of the buffer (not dropped) and retried on
  the next trigger, so transient Postgres errors don't lose data.
- Ordering for retry safety: the (tolerable, approximate) trending `ZINCRBY`
  is written before the authoritative Postgres upsert, so a retry can only
  ever double-count trending, never the source-of-truth totals.
- A larger `BATCH_SIZE` means better write reduction but more data at risk per
  crash. A shorter `FLUSH_INTERVAL_MS` means fresher data, smaller batches.
  The knobs make the durability/throughput trade explicit.

## Performance

Headline numbers from `bun run bench` against the live cluster (full report,
methodology and localhost caveat in [PERFORMANCE.md](./PERFORMANCE.md)):

| Metric                                 | Value                                        |
| -------------------------------------- | -------------------------------------------- |
| `/suggest` latency p95                 | 1.21ms (basic) / 1.35ms (recency)            |
| `/suggest` p99 / mean                  | 2.26ms / 0.63ms                              |
| Throughput (concurrency 32, localhost) | ~50,000 req/s                                |
| Cache hit rate                         | 100% on seeded prefixes (plus live PG fallback) |
| Write reduction from batching          | 90.9x                                        |

```bash
bun run bench                          # latency + hit rate + write reduction
curl -s localhost:8080/metrics | jq    # live cluster counters
```

## API

Full reference with request/response examples is in
[`docs/API.md`](./docs/API.md).

## Screenshots

| Suggestions (debounced, prefix-highlighted)           | Trending board                                  |
| ----------------------------------------------------- | ----------------------------------------------- |
| ![suggestions](./docs/screenshots/ui-suggestions.png) | ![trending](./docs/screenshots/ui-trending.png) |

## Run it

### Full cluster (Docker Compose)

```bash
docker compose up --build
# postgres + redis come up healthy; the `seed` job loads Postgres and derives
# the shard caches, then exits; app1/2/3, cache-updater and lb start.
# open http://localhost:8080
```

Strict networking (`docker-compose.yml`): each `appN` shares a redis network
only with its own `redisN`. App nodes, LB and the cache-updater meet on `mesh`;
Postgres lives on `pg`. The cache-updater touches no Redis at all (it reaches
shards only through the app nodes), so the only component on all three shard
networks is the one-shot `seed` job, which must write Redis directly since it
runs before the app nodes exist.

### Local dev (one Redis with 3 logical DBs + one Postgres)

```bash
docker run -d -p 6379:6379 redis:7-alpine                  # shards = DB 0/1/2
docker run -d -p 5432:5432 -e POSTGRES_USER=typeahead \
  -e POSTGRES_PASSWORD=typeahead -e POSTGRES_DB=typeahead postgres:16-alpine
bun run seed                                               # load PG + derive caches
bun run dev:app1 & bun run dev:app2 & bun run dev:app3 &   # ports 3001/2/3
bun run dev:updater &                                      # cache-updater
bun run dev:lb                                             # http://localhost:8080
```

### Tests, benchmark and demos (cluster must be up)

```bash
bun test              # consistent-hash determinism + balance (no cluster needed)
bun run bench         # p50/p95/p99 latency, cache hit rate, write reduction
bun run demo:recency  # basic-vs-recency ranking, before/after, in logs
```

## Configuration (env)

| Var                                                  | Default            | Purpose                                                |
| ---------------------------------------------------- | ------------------ | ------------------------------------------------------ |
| `DATABASE_URL`                                       | local Postgres     | Central source of truth (Bun.sql)                      |
| `BATCH_SIZE`                                         | `100`              | Buffer size that triggers a batch write                |
| `FLUSH_INTERVAL_MS`                                  | `5000`             | Safety-net flush for partial buffers                   |
| `MAX_PREFIX_LEN`                                     | `32`               | Cap prefix generation (dataset has 500-char junk rows) |
| `CACHE_K`                                            | `50`               | Depth of each derived top-K cache (>= `SUGGEST_LIMIT`) |
| `CACHE_POLL_INTERVAL_MS` / `CACHE_DIRTY_BATCH`       | `1000` / `200`     | Cache-updater cadence and batch size                   |
| `SUGGEST_LIMIT` / `TRENDING_LIMIT`                   | `10` / `10`        | Result counts                                          |
| `DECAY_FACTOR` / `DECAY_INTERVAL_MS`                 | `0.9` / `86400000` | Trending decay                                         |
| `RECENCY_HIST_WEIGHT` / `RECENCY_WEIGHT`             | `1` / `3`          | Blend weights for the recency ranking                  |
| `RECENCY_DECAY_FACTOR` / `RECENCY_DECAY_INTERVAL_MS` | `0.5` / `3600000`  | `recent_count` decay                                   |
| `SHARD_ID`, `PORT`, `REDIS_URL`                      | per node           | App node identity                                      |
| `REDIS_URL_1/2/3`                                    | localhost DBs      | Shard URLs (seeder + updater)                          |
| `APP_URL_1/2/3`, `LB_PORT`                           | localhost          | App node URLs (LB)                                     |
