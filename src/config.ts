/**
 * Shared configuration plus small pure helpers used across the seeder, app
 * nodes and load balancer. Centralised so prefix generation and shard
 * addressing are guaranteed identical everywhere. Bun auto-loads `.env`.
 */

import { SHARDS, type ShardId } from "./hash-ring";

/** Trigger a batch write the moment the in-memory buffer hits this size. */
export const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? 100);

/**
 * Safety-net flush: if a partial buffer sits idle this long it's flushed
 * before reaching BATCH_SIZE, so low-traffic searches don't get stranded.
 * The 100-hit trigger is still the primary path.
 */
export const FLUSH_INTERVAL_MS = Number(process.env.FLUSH_INTERVAL_MS ?? 5000);

/** Default number of autocomplete suggestions returned by GET /suggest. */
export const SUGGEST_LIMIT = Number(process.env.SUGGEST_LIMIT ?? 10);

/** Default number of trending queries returned by GET /trending. */
export const TRENDING_LIMIT = Number(process.env.TRENDING_LIMIT ?? 10);

/**
 * Depth of each derived per-prefix top-K suggestion cache (`q:<prefix>`
 * ZSET). Must be >= SUGGEST_LIMIT; the extra headroom absorbs churn between
 * rebuilds.
 */
export const CACHE_K = Number(process.env.CACHE_K ?? 50);

/**
 * Cap on prefix generation length. Real users never type 200-char
 * autocomplete prefixes, and the dataset contains junk rows up to 500 chars;
 * capping keeps seeding and batch writes bounded.
 */
export const MAX_PREFIX_LEN = Number(process.env.MAX_PREFIX_LEN ?? 32);

/** Time-decay knobs. Interval is overridable for testing. */
export const DECAY_FACTOR = Number(process.env.DECAY_FACTOR ?? 0.9);
export const DECAY_INTERVAL_MS = Number(
  process.env.DECAY_INTERVAL_MS ?? 24 * 60 * 60 * 1000,
);

// ---------------------------------------------------------------------------
// Recency-aware suggestion ranking
// ---------------------------------------------------------------------------
//
// `/suggest` supports two orderings over the same candidate set:
//   - basic   (default)      sorts by all-time `count`.
//   - recency (?rank=recency) sorts by a blended score that lets recently
//                            active queries outrank stale historical giants.
//
// Blended score (computed in SQL by the seeder and cache-updater, stored as
// the `qr:<prefix>` ZSET score so reads stay an O(1) ZREVRANGE):
//
//     score = HIST_WEIGHT * log2(1 + count)
//           + RECENCY_WEIGHT * log2(1 + recent_count)
//
// Both signals are log-compressed (diminishing returns: the millionth search
// matters less than the first), so a burst of recent activity on a modestly
// popular query can overtake an all-time leader that has gone quiet, without
// a single search causing it. That's what makes the reorder demonstrable yet
// stable. `recent_count` is decayed periodically (see below) so spikes fade
// and the order converges back toward all-time popularity.

/** Weight on the all-time popularity term of the blended recency score. */
export const RECENCY_HIST_WEIGHT = Number(process.env.RECENCY_HIST_WEIGHT ?? 1);
/** Weight on the recent-activity term (>1 means recency wins ties). */
export const RECENCY_WEIGHT = Number(process.env.RECENCY_WEIGHT ?? 3);
/** Each decay tick multiplies every non-zero `recent_count` by this factor. */
export const RECENCY_DECAY_FACTOR = Number(process.env.RECENCY_DECAY_FACTOR ?? 0.5);
/** How often the cache-updater decays `recent_count` (default 1h; short for demos). */
export const RECENCY_DECAY_INTERVAL_MS = Number(
  process.env.RECENCY_DECAY_INTERVAL_MS ?? 60 * 60 * 1000,
);

/** Redis key prefix for a suggestion ZSET. `q:<prefix>` -> ZSET(query -> freq). */
export const SUGGEST_KEY_PREFIX = "q:";

/** Redis key prefix for the recency-ranked suggestion ZSET. `qr:<prefix>`. */
export const RECENCY_KEY_PREFIX = "qr:";

/** Redis key for the trending ZSET (one per shard; merged at the LB). */
export const TRENDING_KEY = "trending";

/** Normalise a raw query/prefix: trim + lowercase. Returns "" if blank. */
export function normalize(raw: string): string {
  return raw.normalize("NFC").trim().toLowerCase();
}

/**
 * Generate every valid prefix of a query: "go" -> ["g", "go"]. Capped at
 * MAX_PREFIX_LEN. Assumes `query` is already normalised and non-empty.
 * Uses code-point splitting (via Array.from) to handle Unicode and emoji
 * safely without slicing surrogate pairs in half. Matches Postgres character
 * slicing.
 */
export function prefixesOf(query: string): string[] {
  const chars = Array.from(query);
  const max = Math.min(chars.length, MAX_PREFIX_LEN);
  const prefixes: string[] = new Array(max);
  for (let i = 1; i <= max; i++) {
    prefixes[i - 1] = chars.slice(0, i).join("");
  }
  return prefixes;
}

/** Build the `q:<prefix>` Redis key for the all-time-count suggestion ZSET. */
export function suggestKey(prefix: string): string {
  return SUGGEST_KEY_PREFIX + prefix;
}

/** Build the `qr:<prefix>` Redis key for the recency-blended suggestion ZSET. */
export function recencyKey(prefix: string): string {
  return RECENCY_KEY_PREFIX + prefix;
}

/** The two suggestion-ranking modes `/suggest` understands. */
export type RankMode = "basic" | "recency";

/** Resolve a raw `?rank=` value to a known mode (default `basic`). */
export function rankModeOf(raw: string | null): RankMode {
  return raw === "recency" ? "recency" : "basic";
}

/**
 * Resolve the Redis connection URL for a shard. The seeder needs all three;
 * an app node only ever uses its own (enforced by Docker networking).
 */
export function redisUrlFor(shard: ShardId): string {
  const fromEnv = process.env[`REDIS_URL_${shard}`];
  if (fromEnv) return fromEnv;
  // Local dev default: three logical DBs on one redis instance.
  return `redis://localhost:6379/${Number(shard) - 1}`;
}

/** Resolve the HTTP base URL of the app node paired with a shard. */
export function appUrlFor(shard: ShardId): string {
  return process.env[`APP_URL_${shard}`] ?? `http://localhost:${3000 + Number(shard)}`;
}

/** All shard ids, re-exported for convenience. */
export { SHARDS, type ShardId };
