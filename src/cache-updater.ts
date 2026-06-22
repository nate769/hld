/**
 * Cache updater: the central derived-cache builder.
 *
 * It decides what each per-prefix top-K cache should contain (computed from
 * Postgres, the source of truth) but does not write Redis itself. Instead it
 * POSTs each refreshed cache to the app node that owns the prefix's shard
 * (`POST /internal/cache`), and that node writes its own Redis. So every
 * Redis shard has exactly one writer (its app node); the updater needs only
 * Postgres and the app-node mesh.
 *
 * Loop: poll `dirty_prefixes`, claim a batch via `DELETE ... RETURNING`,
 * recompute each prefix's top-K from Postgres, group by owning shard, then
 * POST each group to that shard's app node. A group whose POST fails is
 * re-marked dirty for the next cycle.
 *
 * Recency decay runs here too: it mutates Postgres (the source of truth)
 * and re-marks affected prefixes so faded spikes actually fall in the
 * served recency cache.
 */

import { route } from "./hash-ring";
import {
  db,
  decayRecentCounts,
  markDirty,
  topKForPrefix,
  topKForPrefixRecency,
  type CountRow,
} from "./db";
import {
  CACHE_K,
  RECENCY_DECAY_FACTOR,
  RECENCY_DECAY_INTERVAL_MS,
  SHARDS,
  appUrlFor,
  prefixesOf,
  type ShardId,
} from "./config";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const POLL_INTERVAL_MS = Number(process.env.CACHE_POLL_INTERVAL_MS ?? 1000);
const DIRTY_BATCH = Number(process.env.CACHE_DIRTY_BATCH ?? 200);

const log = (msg: string) => console.log(`[updater] ${msg}`);
const logErr = (msg: string, err: unknown) => console.error(`[updater] ${msg}`, err);

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface PrefixUpdate {
  prefix: string;
  topK: CountRow[]; // by all-time count -> q:<prefix>
  topKRecency: CountRow[]; // by blended recency score -> qr:<prefix>
}

// ---------------------------------------------------------------------------
// Shard push
// ---------------------------------------------------------------------------

/** POST one shard's batch of cache updates to its app node. */
async function pushToShard(shard: ShardId, updates: PrefixUpdate[]): Promise<void> {
  const res = await fetch(`${appUrlFor(shard)}/internal/cache`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ updates }),
  });
  if (!res.ok) throw new Error(`app${shard} /internal/cache -> ${res.status}`);
}

/** Group prefix updates by the shard that owns each prefix. */
function groupByShard(updates: PrefixUpdate[]): Map<ShardId, PrefixUpdate[]> {
  const grouped = new Map<ShardId, PrefixUpdate[]>();
  for (const update of updates) {
    const shard = route(update.prefix);
    const group = grouped.get(shard) ?? [];
    if (group.length === 0) grouped.set(shard, group);
    group.push(update);
  }
  return grouped;
}

// ---------------------------------------------------------------------------
// Dirty-prefix processing
// ---------------------------------------------------------------------------

/**
 * Atomic claim of the oldest dirty marks. The DELETE-RETURNING sidesteps the
 * SELECT-then-DELETE race without timestamp games: any `/search` that bumps
 * the same prefix mid-flight re-INSERTs a fresh dirty row, so it's
 * reprocessed next cycle. Rebuilds read the source of truth, so a redundant
 * rebuild is harmless.
 */
async function claimDirty(): Promise<string[]> {
  const rows = (await db`
    DELETE FROM dirty_prefixes
    WHERE prefix IN (
      SELECT prefix FROM dirty_prefixes ORDER BY dirty_at ASC LIMIT ${DIRTY_BATCH}
    )
    RETURNING prefix
  `) as { prefix: string }[];
  return rows.map((r) => r.prefix);
}

/** Recompute both rankings for one prefix. */
async function buildUpdate(prefix: string): Promise<PrefixUpdate> {
  const [topK, topKRecency] = await Promise.all([
    topKForPrefix(prefix, CACHE_K),
    topKForPrefixRecency(prefix, CACHE_K),
  ]);
  return { prefix, topK, topKRecency };
}

/**
 * Process up to DIRTY_BATCH dirty prefixes. Returns the count claimed so the
 * caller can keep draining while batches come back full.
 */
async function runCycle(): Promise<number> {
  const prefixes = await claimDirty();
  if (prefixes.length === 0) return 0;

  const updates = await Promise.all(prefixes.map(buildUpdate));

  await Promise.all(
    [...groupByShard(updates)].map(async ([shard, group]) => {
      try {
        await pushToShard(shard, group);
      } catch (err) {
        logErr(
          `shard ${shard} push failed, re-marking ${group.length} dirty:`,
          (err as Error).message,
        );
        await markDirty(group.map((g) => g.prefix));
      }
    }),
  );

  return prefixes.length;
}

// ---------------------------------------------------------------------------
// Loops
// ---------------------------------------------------------------------------

let running = false;

async function tick(): Promise<void> {
  if (running) return; // never overlap cycles
  running = true;
  try {
    // Drain the backlog this tick: keep going while batches come back full.
    let n: number;
    do {
      n = await runCycle();
      if (n > 0) log(`rebuilt ${n} prefix cache(s)`);
    } while (n === DIRTY_BATCH);
  } catch (err) {
    logErr("cycle failed:", err);
  } finally {
    running = false;
  }
}

/**
 * Decay every non-zero `recent_count` and re-mark every affected prefix
 * dirty, so faded spikes actually fall in the served recency cache.
 */
async function runRecencyDecay(): Promise<void> {
  try {
    const affected = await decayRecentCounts(RECENCY_DECAY_FACTOR);
    if (affected.length === 0) return;

    const prefixes = new Set<string>();
    for (const q of affected) for (const p of prefixesOf(q)) prefixes.add(p);
    await markDirty([...prefixes]);
    log(
      `decayed recent_count for ${affected.length} queries; ` +
        `re-marked ${prefixes.size} prefixes for recency-cache rebuild`,
    );
  } catch (err) {
    logErr("recency decay failed:", err);
  }
}

const cycleTimer = setInterval(() => void tick(), POLL_INTERVAL_MS);
const decayTimer = setInterval(() => void runRecencyDecay(), RECENCY_DECAY_INTERVAL_MS);

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

async function shutdown(): Promise<void> {
  log("shutting down ...");
  clearInterval(cycleTimer);
  clearInterval(decayTimer);
  while (running) await new Promise((r) => setTimeout(r, 20));
  await db.close({ timeout: 5 });
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

log(
  `poll=${POLL_INTERVAL_MS}ms batch=${DIRTY_BATCH} K=${CACHE_K} ` +
    `-> app nodes [${SHARDS.map((s) => appUrlFor(s)).join(", ")}]`,
);
