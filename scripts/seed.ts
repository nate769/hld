/**
 * Data loader / seeder. Two-phase.
 *
 * Phase 1: load the dataset into central Postgres `query_counts` (the source
 *          of truth). The seeder is also the single DDL writer (calls
 *          bootstrapSchema).
 * Phase 2: derive the per-prefix top-K suggestion caches from Postgres and
 *          bulk-load them into the Redis shards. Each prefix goes to
 *          `route(prefix)`'s shard, capped at CACHE_K. That's the same
 *          policy the cache-updater uses live, so the cold cache and the
 *          live cache are identical in shape.
 *
 * Run:  bun run scripts/seed.ts
 * Env:  DATABASE_URL, REDIS_URL_1/2/3, SEED_FLUSH=0 to skip the wipe first.
 */

import { RedisClient } from "bun";
import { SHARDS, route, type ShardId } from "../src/hash-ring";
import { normalize, redisUrlFor, suggestKey, recencyKey, CACHE_K } from "../src/config";
import { db, bootstrapSchema, bulkLoadCounts, deriveAllTopK } from "../src/db";

/**
 * Keep a row only if its normalised query has at least one alphanumeric
 * character. The raw dataset's top entry is `"-"` (blank AOL searches,
 * ~98k hits) plus other pure-punctuation noise that would otherwise dominate
 * the `q:` / `q:-` caches with junk. Real single-letter queries ("g", "m")
 * survive.
 */
function isUsableQuery(q: string): boolean {
  return /[a-z0-9]/i.test(q);
}

const DATA_PATH = process.env.DATA_PATH ?? "data/search_frequencies.json";
const CHUNK = Number(process.env.SEED_CHUNK ?? 5000);
const FLUSH_FIRST = process.env.SEED_FLUSH !== "0";

interface Entry {
  query: string;
  count: number;
}

async function main() {
  await bootstrapSchema();
  console.log("[seed] schema ready");

  // Redis shard clients (the seeder is infra: it may touch all shards).
  const clients: Record<ShardId, RedisClient> = {} as Record<ShardId, RedisClient>;
  for (const shard of SHARDS) {
    clients[shard] = new RedisClient(redisUrlFor(shard));
    await clients[shard].connect();
  }

  if (FLUSH_FIRST) {
    console.log("[seed] wiping Postgres tables + Redis shards for a clean reseed ...");
    await db`TRUNCATE query_counts, dirty_prefixes`;
    await Promise.all(SHARDS.map((s) => clients[s].send("FLUSHDB", [])));
  }

  // --- Phase 1: load counts into Postgres ---------------------------------
  console.log(`[seed] reading ${DATA_PATH} ...`);
  const entries: Entry[] = JSON.parse(await Bun.file(DATA_PATH).text());

  // Aggregate by normalised query so duplicates (e.g. "Google" vs "google")
  // merge, and so no single bulk-insert chunk touches the same row twice
  // (which would abort the ON CONFLICT statement).
  const totals = new Map<string, number>();
  let dropped = 0;
  for (const { query, count } of entries) {
    const q = normalize(query);
    if (!q || !isUsableQuery(q)) {
      dropped++;
      continue;
    }
    totals.set(q, (totals.get(q) ?? 0) + count);
  }
  console.log(
    `[seed] ${entries.length.toLocaleString()} rows -> ${totals.size.toLocaleString()} ` +
      `unique queries (${dropped.toLocaleString()} junk/blank rows filtered)`,
  );

  let rows: { query: string; count: number }[] = [];
  let loaded = 0;
  for (const [query, count] of totals) {
    rows.push({ query, count });
    if (rows.length >= CHUNK) {
      await bulkLoadCounts(rows);
      loaded += rows.length;
      rows = [];
      if (loaded % 50000 < CHUNK) console.log(`[seed] loaded ${loaded.toLocaleString()} into Postgres`);
    }
  }
  if (rows.length) {
    await bulkLoadCounts(rows);
    loaded += rows.length;
  }
  console.log(`[seed] Postgres load done: ${loaded.toLocaleString()} queries`);

  // --- Phase 2: derive per-prefix top-K caches into the shards ------------
  console.log(`[seed] deriving top-${CACHE_K} caches into shards ...`);
  const perShard: Record<ShardId, number> = { "1": 0, "2": 0, "3": 0 };
  let pending: Promise<unknown>[] = [];
  let cacheRows = 0;

  const flush = async () => {
    if (pending.length === 0) return;
    const batch = pending;
    pending = [];
    await Promise.all(batch);
  };

  // Seed both caches: `q:<prefix>` (score = count) and `qr:<prefix>`
  // (score = blended recency_score). At cold start recent_count = 0, so the
  // recency order equals the count order; one derivation pass feeds both.
  // The cache-updater diverges them later once live searches bump
  // recent_count.
  for await (const batch of deriveAllTopK(CACHE_K)) {
    for (const { prefix, query, count, recency_score } of batch) {
      const shard = route(prefix);
      pending.push(clients[shard].send("ZADD", [suggestKey(prefix), count, query]));
      pending.push(clients[shard].send("ZADD", [recencyKey(prefix), String(recency_score), query]));
      perShard[shard]++;
      cacheRows++;
      if (pending.length >= CHUNK) await flush();
    }
    await flush();
    console.log(`[seed] derived through len -> ${cacheRows.toLocaleString()} cache rows`);
  }
  await flush();

  console.log(`[seed] done. ${cacheRows.toLocaleString()} cache rows`);
  for (const shard of SHARDS) {
    console.log(`[seed]   shard ${shard}: ${perShard[shard].toLocaleString()} cache rows`);
  }

  for (const shard of SHARDS) clients[shard].close();
  await db.close();
}

main().catch((err) => {
  console.error("[seed] failed:", err);
  process.exit(1);
});
