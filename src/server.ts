/**
 * Application node.
 *
 * A stateless Bun HTTP server paired 1:1 with a single local Redis shard. The
 * load balancer (src/lb.ts) decides routing via the shared consistent-hash
 * ring, so by the time a request lands here it already belongs to this shard.
 *
 * Endpoints:
 *   GET  /suggest?q=<prefix>  top-N suggestions from the local shard.
 *   POST /search  {query}     buffer the query, return 202 immediately.
 *   GET  /trending            top-N trending queries on this shard (LB merges).
 *   GET  /health              shard id + buffer depth (used by compose + LB).
 *   GET  /metrics             per-node counters (LB aggregates).
 *   GET  /cache/debug         hit/miss probe for a single prefix.
 *   POST /internal/cache      apply refreshed top-K caches (cache-updater only).
 *
 * Write path: searches are buffered and flushed in batches, primarily the
 * moment the buffer hits BATCH_SIZE, with a timed safety net for low traffic.
 * A flush UPSERTs counts into central Postgres (the source of truth) and
 * marks each affected prefix dirty. The cache-updater then rebuilds those
 * prefixes' top-K and pushes them back here via POST /internal/cache, so each
 * shard has exactly one writer of its own Redis.
 */

import { RedisClient } from "bun";
import { route } from "./hash-ring";
import {
  db,
  markDirty,
  recordCountsAndDirty,
  topKForPrefix,
  topKForPrefixRecency,
} from "./db";
import {
  BATCH_SIZE,
  CACHE_K,
  DECAY_FACTOR,
  DECAY_INTERVAL_MS,
  FLUSH_INTERVAL_MS,
  SUGGEST_LIMIT,
  TRENDING_KEY,
  TRENDING_LIMIT,
  normalize,
  prefixesOf,
  rankModeOf,
  recencyKey,
  redisUrlFor,
  suggestKey,
  type ShardId,
} from "./config";

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const SHARD_ID = (process.env.SHARD_ID ?? "1") as ShardId;
const PORT = Number(process.env.PORT ?? 3000 + Number(SHARD_ID));
const REDIS_URL = process.env.REDIS_URL ?? redisUrlFor(SHARD_ID);
const MAX_SEARCH_BODY_BYTES = 10 * 1024;
const MAX_CACHE_BODY_BYTES = 10 * 1024 * 1024;
const MAX_BUFFER_DEPTH = 10_000;

const redis = new RedisClient(REDIS_URL);
const log = (msg: string) => console.log(`[app${SHARD_ID}] ${msg}`);
const logErr = (msg: string, err: unknown) => console.error(`[app${SHARD_ID}] ${msg}`, err);

// ---------------------------------------------------------------------------
// In-memory write buffer + batch writer
// ---------------------------------------------------------------------------

/** Pending search queries waiting for a batch flush. */
const buffer: string[] = [];
/** The in-progress drain, if any. Ensures only one drain runs at a time. */
let draining: Promise<void> | null = null;

/**
 * Observability counters, exposed at `GET /metrics` and aggregated by the LB.
 * Approximate, reset on restart. They make the cache hit rate and the write
 * reduction from batching directly measurable.
 */
const metrics = {
  searchesReceived: 0,
  batchesFlushed: 0,
  rowsUpserted: 0,
  cacheHits: 0,
  cacheMisses: 0,
};

/** Aggregate raw queries into per-query counts. */
function aggregateCounts(queries: string[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const q of queries) counts.set(q, (counts.get(q) ?? 0) + 1);
  return counts;
}

/** Collect every prefix affected by the given queries (deduped). */
function affectedPrefixes(queries: Iterable<string>): Set<string> {
  const prefixes = new Set<string>();
  for (const q of queries) for (const p of prefixesOf(q)) prefixes.add(p);
  return prefixes;
}

/**
 * Write one BATCH_SIZE chunk.
 *
 *   1. Dedup the chunk's counts (Postgres `ON CONFLICT` can't touch the same
 *      row twice in one statement).
 *   2. Bump per-shard trending in Redis first (one auto-pipelined round-trip).
 *      Doing this before the authoritative write keeps retries safe: a retry
 *      can only ever double-count the (tolerable) trending signal, never the
 *      durable Postgres totals.
 *   3. UPSERT counts into Postgres and mark every affected prefix dirty in the
 *      same transaction.
 *
 * On failure the chunk is re-queued at the front of the buffer; the caller
 * stops the drain so we don't hot-loop.
 */
async function writeChunk(batch: string[]): Promise<void> {
  const counts = aggregateCounts(batch);
  const prefixes = affectedPrefixes(counts.keys());

  await Promise.all(
    [...counts].map(([query, count]) =>
      redis.send("ZINCRBY", [TRENDING_KEY, String(count), query]),
    ),
  );

  await recordCountsAndDirty(counts, prefixes);

  metrics.batchesFlushed++;
  metrics.rowsUpserted += counts.size;
  log(
    `flushed ${batch.length} searches ` +
      `(${counts.size} unique, ${prefixes.size} prefixes dirtied) ` +
      `[batch #${metrics.batchesFlushed}]`,
  );
}

/** Drain the whole buffer in BATCH_SIZE chunks, including new arrivals. */
async function drain(): Promise<void> {
  while (buffer.length > 0) {
    const batch = buffer.splice(0, BATCH_SIZE);
    try {
      await writeChunk(batch);
    } catch (err) {
      logErr("batch flush failed, re-queuing:", err);
      buffer.unshift(...batch); // retry next trigger; don't hot-loop
      return;
    }
  }
}

/** Start a drain if one isn't already running. */
function scheduleFlush(): Promise<void> {
  if (!draining) draining = drain().finally(() => (draining = null));
  return draining;
}

// Safety-net flush so a partial buffer never gets stranded on low traffic.
setInterval(() => {
  if (buffer.length > 0) void scheduleFlush();
}, FLUSH_INTERVAL_MS);

// ---------------------------------------------------------------------------
// Time decay
// ---------------------------------------------------------------------------

/**
 * Atomically multiply every trending score by DECAY_FACTOR (default 0.9, a
 * 10% daily decay) so historically huge queries gradually yield to recently
 * active ones. A Lua script keeps read-modify-write atomic.
 */
const DECAY_LUA = `
local key = KEYS[1]
local factor = tonumber(ARGV[1])
local items = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
for i = 1, #items, 2 do
  redis.call('ZADD', key, tonumber(items[i + 1]) * factor, items[i])
end
return #items / 2
`;

async function runDecay(): Promise<void> {
  try {
    const n = await redis.send("EVAL", [DECAY_LUA, "1", TRENDING_KEY, String(DECAY_FACTOR)]);
    log(`applied ${DECAY_FACTOR}x decay to ${n} trending entries`);
  } catch (err) {
    logErr("decay failed:", err);
  }
}

setInterval(() => void runDecay(), DECAY_INTERVAL_MS);

// ---------------------------------------------------------------------------
// Derived suggestion cache (written only via POST /internal/cache)
// ---------------------------------------------------------------------------

/**
 * Atomically replace a prefix cache: DEL then ZADD top-K in one Lua call, so
 * /suggest never observes an empty key mid-rebuild.
 * ARGV layout = [score1, member1, score2, member2, ...].
 */
const REPLACE_LUA = `
redis.call('DEL', KEYS[1])
for i = 1, #ARGV, 2 do
  redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i + 1])
end
return 1
`;

interface ScoredEntry {
  query: string;
  count: string | number;
}

interface CacheUpdate {
  prefix: string;
  /** Top-K ordered by all-time count -> `q:<prefix>` ZSET. */
  topK: ScoredEntry[];
  /** Top-K ordered by the blended recency score -> `qr:<prefix>` ZSET. */
  topKRecency?: ScoredEntry[];
}

/** Flatten a scored top-K into the `[score, member, ...]` ARGV REPLACE_LUA wants. */
function toArgv(entries: ScoredEntry[]): string[] {
  const argv: string[] = [];
  for (const { query, count } of entries) argv.push(String(count), query);
  return argv;
}

/**
 * Apply a batch of cache replacements. Each prefix carries both rankings
 * (`q:<prefix>` and `qr:<prefix>`), replaced atomically. Prefixes this shard
 * doesn't own are rejected, so a misrouted update can never plant a key on
 * the wrong shard.
 */
async function applyCacheUpdates(
  updates: CacheUpdate[],
): Promise<{ applied: number; rejected: number }> {
  const pipeline: Promise<unknown>[] = [];
  let applied = 0;
  let rejected = 0;
  for (const { prefix, topK, topKRecency } of updates) {
    if (route(prefix) !== SHARD_ID) {
      rejected++;
      continue;
    }
    pipeline.push(redis.send("EVAL", [REPLACE_LUA, "1", suggestKey(prefix), ...toArgv(topK)]));
    if (topKRecency) {
      pipeline.push(
        redis.send("EVAL", [REPLACE_LUA, "1", recencyKey(prefix), ...toArgv(topKRecency)]),
      );
    }
    applied++;
  }
  await Promise.all(pipeline);
  return { applied, rejected };
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

const json = (data: unknown, status = 200) =>
  Response.json(data, { status, headers: { "access-control-allow-origin": "*" } });

/** Parse a positive-integer limit param, falling back to `def`, capped. */
function parseLimit(raw: string | null, def: number, max = Math.max(CACHE_K, 100)): number {
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? Math.min(n, max) : def;
}

/**
 * Parse a `... WITHSCORES` reply into {query, score} objects. Bun speaks
 * RESP3, returning [member, score] tuples. We also tolerate the flat RESP2
 * layout for safety.
 */
function parseScored(raw: unknown): { query: string; score: number }[] {
  if (!Array.isArray(raw)) return [];
  if (raw.length > 0 && Array.isArray(raw[0])) {
    return (raw as [string, number][]).map(([query, score]) => ({
      query,
      score: Number(score),
    }));
  }
  const out: { query: string; score: number }[] = [];
  for (let i = 0; i < raw.length; i += 2) {
    out.push({ query: String(raw[i]), score: Number(raw[i + 1]) });
  }
  return out;
}

function contentLengthOf(req: Request): number {
  return Number(req.headers.get("content-length") ?? 0);
}

async function readJsonBody<T>(req: Request, maxBytes: number): Promise<T | Response> {
  if (contentLengthOf(req) > maxBytes) {
    return json({ error: "request entity too large" }, 413);
  }
  try {
    return (await req.json()) as T;
  } catch {
    return json({ error: "invalid json body" }, 400);
  }
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

async function handleSuggest(url: URL): Promise<Response> {
  const prefix = normalize(url.searchParams.get("q") ?? "");
  if (!prefix) return json({ prefix: "", suggestions: [], source: "cache" });

  const limit = parseLimit(url.searchParams.get("limit"), SUGGEST_LIMIT);
  const rank = rankModeOf(url.searchParams.get("rank"));
  const key = rank === "recency" ? recencyKey(prefix) : suggestKey(prefix);

  let suggestions: string[] = [];
  let source = "cache";

  try {
    suggestions = (await redis.send("ZREVRANGE", [key, "0", String(limit - 1)])) as string[];
  } catch (err) {
    logErr("Redis suggestions fetch failed:", err);
    // Treat as a cache miss; fall through to Postgres.
  }

  if (suggestions.length === 0) {
    metrics.cacheMisses++;
    const rows =
      rank === "recency"
        ? await topKForPrefixRecency(prefix, limit)
        : await topKForPrefix(prefix, limit);
    suggestions = rows.map((r) => r.query);
    source = "db";
    if (suggestions.length > 0) markDirty([prefix]).catch(() => {});
  } else {
    metrics.cacheHits++;
  }

  return json({ shard: SHARD_ID, prefix, rank, source, suggestions });
}

async function handleSearch(req: Request): Promise<Response> {
  if (buffer.length >= MAX_BUFFER_DEPTH) {
    return json({ error: "Search buffer full, please retry later" }, 503);
  }

  const body = await readJsonBody<{ query?: string }>(req, MAX_SEARCH_BODY_BYTES);
  if (body instanceof Response) return body;

  const query = normalize(body.query ?? "");
  if (!query) return json({ error: "missing query" }, 400);

  metrics.searchesReceived++;
  const buffered = buffer.push(query);
  if (buffered >= BATCH_SIZE) void scheduleFlush();

  return json({ message: "Searched", query, buffered }, 202);
}

async function handleTrending(url: URL): Promise<Response> {
  const limit = parseLimit(url.searchParams.get("limit"), TRENDING_LIMIT);
  const raw = await redis.send("ZREVRANGE", [
    TRENDING_KEY,
    "0",
    String(limit - 1),
    "WITHSCORES",
  ]);
  return json({ shard: SHARD_ID, trending: parseScored(raw) });
}

async function handleCacheDebug(url: URL): Promise<Response> {
  const prefix = normalize(url.searchParams.get("prefix") ?? "");
  if (!prefix) return json({ error: "missing prefix" }, 400);

  const [cachedRaw, recencyRaw] = await Promise.all([
    redis.send("ZCARD", [suggestKey(prefix)]),
    redis.send("ZCARD", [recencyKey(prefix)]),
  ]);
  const cached = Number(cachedRaw);
  const recencyCached = Number(recencyRaw);
  return json({
    prefix,
    node: `app${SHARD_ID}`,
    shard: SHARD_ID,
    status: cached > 0 ? "hit" : "miss",
    cached,
    recencyCached,
  });
}

function handleMetrics(): Response {
  const suggestTotal = metrics.cacheHits + metrics.cacheMisses;
  return json({
    shard: SHARD_ID,
    buffered: buffer.length,
    ...metrics,
    cacheHitRate: suggestTotal > 0 ? metrics.cacheHits / suggestTotal : null,
    writeReduction:
      metrics.batchesFlushed > 0 ? metrics.searchesReceived / metrics.batchesFlushed : null,
  });
}

async function handleInternalCache(req: Request): Promise<Response> {
  const body = await readJsonBody<{ updates?: CacheUpdate[] }>(req, MAX_CACHE_BODY_BYTES);
  if (body instanceof Response) return body;

  const result = await applyCacheUpdates(body.updates ?? []);
  return json({ shard: SHARD_ID, ...result });
}

async function handleHealth(): Promise<Response> {
  const [redisUp, pgUp] = await Promise.all([
    redis.send("PING", []).then(() => true).catch(() => false),
    db`SELECT 1`.then(() => true).catch(() => false),
  ]);
  return json(
    {
      shard: SHARD_ID,
      buffered: buffer.length,
      redis: redisUp ? "up" : "down",
      postgres: pgUp ? "up" : "down",
    },
    redisUp ? 200 : 503,
  );
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

const server = Bun.serve({
  port: PORT,
  async fetch(req) {
    const url = new URL(req.url);
    const { pathname } = url;
    const { method } = req;

    if (method === "GET" && pathname === "/suggest") return handleSuggest(url);
    if (method === "POST" && pathname === "/search") return handleSearch(req);
    if (method === "GET" && pathname === "/trending") return handleTrending(url);
    if (method === "GET" && pathname === "/cache/debug") return handleCacheDebug(url);
    if (method === "GET" && pathname === "/metrics") return handleMetrics();
    if (method === "POST" && pathname === "/internal/cache") return handleInternalCache(req);
    if (method === "GET" && pathname === "/health") return handleHealth();

    return json({ error: "not found" }, 404);
  },
});

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

async function shutdown(): Promise<void> {
  log("shutting down, flushing buffer ...");
  server.stop();
  if (draining) await draining;
  await scheduleFlush();
  redis.close();
  await db.close({ timeout: 5 });
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

log(`listening on :${server.port} -> redis ${REDIS_URL} (route("a")=${route("a")})`);
