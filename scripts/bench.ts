/**
 * Benchmark harness.
 *
 * Drives a running cluster through the load balancer and reports:
 *   - /suggest latency: p50 / p90 / p95 / p99 / max over many requests
 *   - cache hit rate:   hits vs Postgres fallbacks (from /metrics)
 *   - write reduction:  searches accepted per Postgres batch (from /metrics)
 *
 * Uses only the public HTTP API, so the numbers reflect the real proxy +
 * shard + Redis path. Pure measurement, no internal hooks.
 *
 * Run (the cluster must be up, e.g. `docker compose up`):
 *   bun run scripts/bench.ts
 *   BASE_URL=http://localhost:8080 BENCH_SUGGEST=5000 BENCH_RANK=recency bun run scripts/bench.ts
 */

import { normalize } from "../src/config";

const BASE = process.env.BASE_URL ?? "http://localhost:8080";
const N_SUGGEST = Number(process.env.BENCH_SUGGEST ?? 3000);
const N_SEARCH = Number(process.env.BENCH_SEARCH ?? 1000);
const CONCURRENCY = Number(process.env.BENCH_CONCURRENCY ?? 32);
const RANK = process.env.BENCH_RANK ?? "basic"; // basic | recency
const DATA_PATH = process.env.DATA_PATH ?? "data/search_frequencies.json";

interface Entry {
  query: string;
  count: number;
}

/** Build a pool of realistic 2-4 char prefixes from the most popular queries. */
async function buildPrefixPool(): Promise<string[]> {
  const entries: Entry[] = JSON.parse(await Bun.file(DATA_PATH).text());
  const usable = entries
    .map((e) => normalize(e.query))
    .filter((q) => /[a-z0-9]/i.test(q) && q.length >= 2);
  const pool = new Set<string>();
  for (const q of usable) {
    const len = 2 + (q.length % 3); // 2..4
    pool.add(q.slice(0, Math.min(len, q.length)));
    if (pool.size >= 20000) break;
  }
  return [...pool];
}

/** Run `total` async tasks with at most `concurrency` in flight. */
async function pooled<T>(total: number, concurrency: number, task: (i: number) => Promise<T>) {
  let next = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      const i = next++;
      if (i >= total) return;
      await task(i);
    }
  });
  await Promise.all(workers);
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, idx)]!;
}

const ms = (n: number) => `${n.toFixed(2)}ms`;

async function getMetrics(): Promise<any> {
  const r = await fetch(`${BASE}/metrics`);
  return r.json();
}

async function benchSuggest(pool: string[]) {
  console.log(`\n> /suggest latency  (rank=${RANK}, n=${N_SUGGEST}, concurrency=${CONCURRENCY})`);

  // Warm-up so JIT and connections are hot, and we measure steady state.
  await pooled(Math.min(200, N_SUGGEST), CONCURRENCY, async (i) => {
    await fetch(`${BASE}/suggest?q=${encodeURIComponent(pool[i % pool.length]!)}&rank=${RANK}`);
  });

  const latencies: number[] = [];
  let hits = 0;
  let dbFallbacks = 0;
  let errors = 0;

  const t0 = performance.now();
  await pooled(N_SUGGEST, CONCURRENCY, async (i) => {
    const prefix = pool[Math.floor(Math.random() * pool.length)]!;
    const start = performance.now();
    try {
      const res = await fetch(`${BASE}/suggest?q=${encodeURIComponent(prefix)}&rank=${RANK}`);
      const body = (await res.json()) as { source?: string };
      latencies.push(performance.now() - start);
      if (body.source === "db") dbFallbacks++;
      else hits++;
    } catch {
      errors++;
    }
  });
  const wall = performance.now() - t0;

  latencies.sort((a, b) => a - b);
  const sum = latencies.reduce((a, b) => a + b, 0);
  console.log(`  requests   : ${latencies.length} ok, ${errors} errors`);
  console.log(`  throughput : ${(latencies.length / (wall / 1000)).toFixed(0)} req/s (wall ${ms(wall)})`);
  console.log(`  mean       : ${ms(sum / latencies.length)}`);
  console.log(`  p50        : ${ms(percentile(latencies, 50))}`);
  console.log(`  p90        : ${ms(percentile(latencies, 90))}`);
  console.log(`  p95        : ${ms(percentile(latencies, 95))}`);
  console.log(`  p99        : ${ms(percentile(latencies, 99))}`);
  console.log(`  max        : ${ms(latencies[latencies.length - 1] ?? 0)}`);
  console.log(`  cache hits : ${hits} / ${hits + dbFallbacks} (${((hits / (hits + dbFallbacks)) * 100).toFixed(1)}% this run)`);
}

async function benchWriteReduction(pool: string[]) {
  console.log(`\n> write reduction  (n=${N_SEARCH} searches)`);
  const before = await getMetrics();

  // Submit N searches; reuse a small set of queries so dedup shows in logs too.
  const queries = pool.slice(0, 50).map((p) => `${p} demo`);
  await pooled(N_SEARCH, CONCURRENCY, async (i) => {
    const q = queries[i % queries.length]!;
    await fetch(`${BASE}/search`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query: q }),
    });
  });

  // Let the buffers flush (poll until drained, bounded wait).
  for (let i = 0; i < 40; i++) {
    const m = await getMetrics();
    if ((m.total?.buffered ?? 0) === 0) break;
    await Bun.sleep(250);
  }
  const after = await getMetrics();

  const dSearches = (after.total.searchesReceived ?? 0) - (before.total.searchesReceived ?? 0);
  const dBatches = (after.total.batchesFlushed ?? 0) - (before.total.batchesFlushed ?? 0);
  const dRows = (after.total.rowsUpserted ?? 0) - (before.total.rowsUpserted ?? 0);
  console.log(`  searches submitted     : ${dSearches}`);
  console.log(`  Postgres batch txns    : ${dBatches}`);
  console.log(`  unique rows upserted   : ${dRows}`);
  console.log(
    `  write reduction        : ${dBatches > 0 ? (dSearches / dBatches).toFixed(1) : "n/a"}x ` +
      `(searches per DB transaction)`,
  );
}

async function main() {
  console.log(`# Typeahead benchmark against ${BASE}`);
  try {
    await fetch(`${BASE}/metrics`);
  } catch {
    console.error(`Cannot reach ${BASE}. Is the cluster up? (docker compose up)`);
    process.exit(1);
  }

  const pool = await buildPrefixPool();
  console.log(`Prefix pool: ${pool.length} distinct prefixes`);

  await benchSuggest(pool);
  await benchWriteReduction(pool);

  const m = await getMetrics();
  console.log(`\n> cluster /metrics (cumulative)`);
  console.log(`  cache hit rate  : ${m.total.cacheHitRate != null ? (m.total.cacheHitRate * 100).toFixed(1) + "%" : "n/a"}`);
  console.log(`  write reduction : ${m.total.writeReduction != null ? m.total.writeReduction.toFixed(1) + "x" : "n/a"}`);
  console.log(`  searches=${m.total.searchesReceived} batches=${m.total.batchesFlushed} rows=${m.total.rowsUpserted}`);
  console.log(`  hits=${m.total.cacheHits} misses=${m.total.cacheMisses}`);
}

main();
