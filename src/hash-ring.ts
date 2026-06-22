/**
 * Consistent hashing ring.
 *
 * A deterministic router that maps an arbitrary string (a search prefix or a
 * full query) onto one of a fixed set of shards. The same ring construction
 * is used by three independent processes:
 *
 *   - `scripts/seed.ts` decides which Redis shard a prefix is bulk-loaded into.
 *   - `src/lb.ts`       decides which app node a request is proxied to.
 *   - `src/server.ts`   decides which shard owns a prefix during batch writes.
 *
 * Because every process imports this module and builds the ring from the
 * same `SHARDS` list and `VIRTUAL_NODES` count, `route(key)` is globally
 * consistent: a given prefix always resolves to the same shard everywhere.
 *
 * Virtual nodes (replicas) spread the keyspace evenly across the physical
 * shards instead of clumping. See `VIRTUAL_NODES`.
 */

/** The logical shard ids. App node `i` is paired 1:1 with `redis<i>`. */
export const SHARDS = ["1", "2", "3"] as const;
export type ShardId = (typeof SHARDS)[number];

/**
 * Replicas per physical shard placed around the ring. Higher means smoother
 * distribution at the cost of a larger ring. 150 is a common production value.
 */
export const VIRTUAL_NODES = 150;

/**
 * FNV-1a (32-bit): a fast, well-distributed, dependency-free string hash.
 * Returns an unsigned 32-bit integer so ring positions are stable and
 * comparable across processes and runtimes. Computes over UTF-8 bytes to
 * stay aligned with external tools.
 */
export function fnv1a32(str: string): number {
  const encoder = new TextEncoder();
  const bytes = encoder.encode(str);
  let hash = 0x811c9dc5; // FNV offset basis
  for (let i = 0; i < bytes.length; i++) {
    hash ^= bytes[i]!;
    // hash *= 16777619 (FNV prime), done with shifts to stay in 32-bit range
    hash +=
      (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return hash >>> 0; // coerce to unsigned 32-bit
}

interface RingEntry {
  hash: number;
  shard: ShardId;
}

export class HashRing {
  /** Ring entries sorted ascending by hash for binary search. */
  private readonly ring: RingEntry[] = [];

  constructor(
    shards: readonly string[] = SHARDS,
    private readonly virtualNodes: number = VIRTUAL_NODES,
  ) {
    for (const shard of shards) {
      for (let v = 0; v < this.virtualNodes; v++) {
        this.ring.push({ hash: fnv1a32(`${shard}#${v}`), shard: shard as ShardId });
      }
    }
    this.ring.sort((a, b) => a.hash - b.hash);
  }

  /**
   * Map a key to the shard that owns it: hash the key, then walk clockwise
   * to the first ring entry whose position is >= the key's hash (wrapping
   * around to the start of the ring). Implemented as a binary search,
   * O(log n).
   */
  route(key: string): ShardId {
    if (this.ring.length === 0) {
      throw new Error("Cannot route key: HashRing has no active shards");
    }
    const h = fnv1a32(key);
    const ring = this.ring;

    let lo = 0;
    let hi = ring.length - 1;
    // Find the first entry with hash >= h.
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (ring[mid]!.hash < h) lo = mid + 1;
      else hi = mid;
    }
    // If h is greater than every entry, wrap to the first entry on the ring.
    const entry = ring[lo]!;
    return entry.hash >= h ? entry.shard : ring[0]!.shard;
  }
}

/** Shared singleton ring built from the canonical `SHARDS` list. */
export const ring = new HashRing();

/** Convenience: route a key using the shared ring. */
export function route(key: string): ShardId {
  return ring.route(key);
}
