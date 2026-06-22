/**
 * Central SQL layer (Bun.sql / Postgres). The durable source of truth.
 *
 * Postgres holds the authoritative per-query totals (`query_counts`). The
 * Redis shards are a derived per-prefix top-K cache rebuilt from this table
 * by the seeder (cold start) and the cache-updater (live). App nodes write
 * counts here and mark affected prefixes dirty; they never write the
 * suggestion cache.
 *
 * Bun.sql behaviours this module relies on (probed against PG 16):
 *   - the `${sql(rows, ...cols)}` bulk-insert helper composes with
 *     `ON CONFLICT ... DO UPDATE`;
 *   - a row touched twice in one INSERT aborts it ("cannot affect row a
 *     second time"), so callers must dedup (Map for counts, Set for prefixes);
 *   - `BIGINT` columns come back as JS strings (handy, since ZADD wants a
 *     string);
 *   - DDL with multiple statements must go through `.simple()` (no params).
 */

import { SQL } from "bun";
import { MAX_PREFIX_LEN, RECENCY_HIST_WEIGHT, RECENCY_WEIGHT } from "./config";

export const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://typeahead:typeahead@localhost:5432/typeahead";

/** Pooled client (one per process). */
export const db = new SQL(DATABASE_URL, { max: Number(process.env.PG_POOL_MAX ?? 10) });

/** A row of the top-K cache. `count` is a string because BIGINT -> string. */
export interface CountRow {
  query: string;
  count: string;
}

/**
 * The blended recency score as a raw SQL fragment. This is the single source
 * of truth for the formula, reused by the live recency query, the cold-start
 * derivation and the cache-updater, so every path ranks identically.
 *
 *     HIST_WEIGHT * log2(1 + count) + RECENCY_WEIGHT * log2(1 + recent_count)
 *
 * Both weights are Number()-coerced config values (never user input), so
 * inlining them is injection-safe. log2 compresses each signal (diminishing
 * returns) so a burst of recent activity can overtake a stale all-time
 * leader, but not on a single hit. See `config.ts` for the full rationale.
 */
const HW = Number.isFinite(RECENCY_HIST_WEIGHT) ? RECENCY_HIST_WEIGHT : 1;
const RW = Number.isFinite(RECENCY_WEIGHT) ? RECENCY_WEIGHT : 3;
export const RECENCY_SCORE_SQL = `(${HW} * log(2.0, (1 + count)::numeric) + ${RW} * log(2.0, (1 + recent_count)::numeric))`;

/**
 * Create the schema if absent. The seeder is the single DDL writer (so app
 * nodes and the updater never race on CREATE INDEX), but it's idempotent.
 *
 * `query_counts_query_pattern_idx` uses `text_pattern_ops` so that
 * `query LIKE 'prefix%'` is a range scan regardless of the DB collation.
 */
export async function bootstrapSchema(client: SQL = db): Promise<void> {
  await client`
    CREATE TABLE IF NOT EXISTS query_counts (
      query        TEXT PRIMARY KEY,
      count        BIGINT NOT NULL,
      recent_count BIGINT NOT NULL DEFAULT 0
    );
    -- recency signal: a decaying counter of recent activity.
    -- ADD COLUMN keeps the schema idempotent if an older table already exists.
    ALTER TABLE query_counts ADD COLUMN IF NOT EXISTS recent_count BIGINT NOT NULL DEFAULT 0;
    CREATE INDEX IF NOT EXISTS query_counts_query_pattern_idx
      ON query_counts (query text_pattern_ops);
    CREATE TABLE IF NOT EXISTS dirty_prefixes (
      prefix   TEXT PRIMARY KEY,
      dirty_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `.simple();
}

/**
 * Escape LIKE metacharacters so a user prefix is matched literally:
 * `_` and `%` are wildcards, `\` is the escape char. Pair with `ESCAPE '\'`.
 */
export function escapeLike(prefix: string): string {
  return prefix.replace(/[\\%_]/g, (c) => "\\" + c);
}

/**
 * Write path (app nodes), in one transaction so a dirty mark is never
 * visible to the updater without its count already committed:
 *   1. accumulate per-query deltas (UPSERT add),
 *   2. bump dirty_at=now() for every affected prefix.
 * `counts` and `prefixes` must already be deduped by the caller.
 */
export async function recordCountsAndDirty(
  counts: Map<string, number>,
  prefixes: Set<string>,
): Promise<void> {
  if (counts.size === 0) return;
  // recent_count is seeded with the same delta as count so a brand-new query
  // is immediately "recent"; on conflict both grow by the delta. recent_count
  // is later decayed (see decayRecentCounts) while count is permanent.
  const countRows = [...counts].map(([query, count]) => ({ query, count, recent_count: count }));
  const prefixRows = [...prefixes].map((prefix) => ({ prefix }));

  await db.begin(async (tx) => {
    await tx`
      INSERT INTO query_counts ${tx(countRows, "query", "count", "recent_count")}
      ON CONFLICT (query) DO UPDATE
        SET count = query_counts.count + EXCLUDED.count,
            recent_count = query_counts.recent_count + EXCLUDED.recent_count
    `;
    if (prefixRows.length > 0) {
      await tx`
        INSERT INTO dirty_prefixes ${tx(prefixRows, "prefix")}
        ON CONFLICT (prefix) DO UPDATE SET dirty_at = now()
      `;
    }
  });
}

/**
 * Re-mark prefixes dirty (no count change). The cache-updater uses this to
 * requeue a batch whose shard write failed, so it's retried on a later cycle.
 */
export async function markDirty(prefixes: string[], client: SQL = db): Promise<void> {
  if (prefixes.length === 0) return;
  const uniquePrefixes = [...new Set(prefixes)];
  const rows = uniquePrefixes.map((prefix) => ({ prefix }));
  await client`
    INSERT INTO dirty_prefixes ${client(rows, "prefix")}
    ON CONFLICT (prefix) DO UPDATE SET dirty_at = now()
  `;
}

/**
 * The top-K queries for a single prefix, by count desc with `query ASC` as a
 * deterministic tiebreak. The tiebreak must match the seeder's window
 * function so re-derivation is stable. Uses the constant-prefix LIKE range
 * scan.
 */
export async function topKForPrefix(
  prefix: string,
  k: number,
  client: SQL = db,
): Promise<CountRow[]> {
  return (await client`
    SELECT query, count FROM query_counts
    WHERE query LIKE ${escapeLike(prefix) + "%"} ESCAPE ${"\\"}
    ORDER BY count DESC, query ASC
    LIMIT ${k}
  `) as CountRow[];
}

/**
 * The top-K queries for a single prefix ranked by the blended recency score.
 * Ordered over all candidates for the prefix (not just the count-leaders) so
 * a low-all-time but currently-hot query can surface. `count` in the returned
 * rows is the blended score (stored as the `qr:<prefix>` ZSET score). Same
 * deterministic `query ASC` tiebreak as the basic ranking. `unsafe` inlines
 * `RECENCY_SCORE_SQL`; the prefix and k are parameterised.
 */
export async function topKForPrefixRecency(
  prefix: string,
  k: number,
  client: SQL = db,
): Promise<CountRow[]> {
  return (await client.unsafe(
    `SELECT query, round(${RECENCY_SCORE_SQL}, 6)::float8 AS count
       FROM query_counts
      WHERE query LIKE $1 ESCAPE '\\'
      ORDER BY ${RECENCY_SCORE_SQL} DESC, query ASC
      LIMIT $2`,
    [escapeLike(prefix) + "%", k],
  )) as CountRow[];
}

/**
 * Decay every non-zero `recent_count` by `factor` (e.g. 0.5) and return the
 * queries that were affected. The caller re-derives those queries' prefixes
 * so the served recency cache actually reflects the decay. Otherwise a query
 * that spiked then went silent would keep its inflated rank forever.
 * `floor` lets a fading spike reach 0 and drop out, so the recency order
 * converges back toward all-time popularity.
 */
export async function decayRecentCounts(
  factor: number,
  client: SQL = db,
): Promise<string[]> {
  const rows = (await client.unsafe(
    `UPDATE query_counts
        SET recent_count = floor(recent_count * $1)
      WHERE recent_count > 0
      RETURNING query`,
    [factor],
  )) as { query: string }[];
  return rows.map((r) => r.query);
}

/** A derived cache row: the top-K for `prefix`. `recency_score` is the blend. */
export interface DerivedRow extends CountRow {
  prefix: string;
  recency_score: number;
}

/**
 * Derive the top-K for every distinct prefix from `query_counts`, used for
 * the cold-start cache build. Chunked by prefix length (1..MAX_PREFIX_LEN):
 * each pass ranks all prefixes of one length with a single window-function
 * scan, so peak memory is one length class rather than all ~1.6M rows at
 * once. Yields batches; the caller pipelines them to the owning shards.
 */
export async function* deriveAllTopK(
  k: number,
  client: SQL = db,
): AsyncGenerator<DerivedRow[]> {
  for (let len = 1; len <= MAX_PREFIX_LEN; len++) {
    // At cold start `recent_count = 0`, so the recency score is monotonic in
    // count and the count-ordered top-K is also the recency top-K: both
    // shard caches can be seeded from one pass. `recency_score` shares
    // `RECENCY_SCORE_SQL` with the live path so there's no formula drift.
    // `unsafe` is needed because that fragment is raw SQL; $1 (len) is
    // reused, $2 is k.
    const batch = (await client.unsafe(
      `WITH expanded AS (
         SELECT query, count, recent_count, left(query, $1) AS prefix
         FROM query_counts
         WHERE length(query) >= $1
       ), ranked AS (
         SELECT prefix, query, count, recent_count,
                row_number() OVER (PARTITION BY prefix ORDER BY count DESC, query ASC) AS rn
         FROM expanded
       )
       SELECT prefix, query, count,
              round(${RECENCY_SCORE_SQL}, 6)::float8 AS recency_score
       FROM ranked WHERE rn <= $2`,
      [len, k],
    )) as DerivedRow[];
    if (batch.length > 0) yield batch;
  }
}

/** Bulk-insert raw dataset counts (seeder). Caller chunks and dedups. */
export async function bulkLoadCounts(
  rows: { query: string; count: number }[],
  client: SQL = db,
): Promise<void> {
  if (rows.length === 0) return;
  await client`
    INSERT INTO query_counts ${client(rows, "query", "count")}
    ON CONFLICT (query) DO UPDATE
      SET count = query_counts.count + EXCLUDED.count
  `;
}
