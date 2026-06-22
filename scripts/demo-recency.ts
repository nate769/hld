/**
 * Recency-ranking demo.
 *
 * For one prefix, prints the suggestion list under both rankings, then drives
 * a burst of searches for a modestly-popular query sharing that prefix, waits
 * for the write -> Postgres -> cache-updater pipeline to settle, and re-prints
 * both lists. Expected result:
 *   - rank=basic   (all-time count) is essentially unchanged; history wins.
 *   - rank=recency (blended score)  has the bursted query climb to the top.
 *
 * Run (cluster up):  bun run scripts/demo-recency.ts
 *   DEMO_PREFIX=go DEMO_QUERY="go surge demo" DEMO_BURST=400 bun run scripts/demo-recency.ts
 */

const BASE = process.env.BASE_URL ?? "http://localhost:8080";
const PREFIX = (process.env.DEMO_PREFIX ?? "go").toLowerCase();
const HOT = (process.env.DEMO_QUERY ?? `${PREFIX} surge demo`).toLowerCase();
const BURST = Number(process.env.DEMO_BURST ?? 400);
const LIMIT = Number(process.env.DEMO_LIMIT ?? 10);

async function suggest(rank: "basic" | "recency"): Promise<string[]> {
  const res = await fetch(`${BASE}/suggest?q=${encodeURIComponent(PREFIX)}&rank=${rank}&limit=${LIMIT}`);
  const body = (await res.json()) as { suggestions?: string[] };
  return body.suggestions ?? [];
}

function table(basic: string[], recency: string[]): string {
  const rows: string[] = [];
  const n = Math.max(basic.length, recency.length);
  const mark = (list: string[], i: number) =>
    (list[i] === HOT ? "> " : "  ") + (list[i] ?? "");
  rows.push(`  #   rank=basic (count)              rank=recency (blended)`);
  rows.push(`  --  ------------------------------  ------------------------------`);
  for (let i = 0; i < n; i++) {
    const b = mark(basic, i).padEnd(32);
    const r = mark(recency, i);
    rows.push(`  ${String(i + 1).padStart(2)}  ${b}${r}`);
  }
  return rows.join("\n");
}

function rankOf(list: string[]): string {
  const i = list.indexOf(HOT);
  return i < 0 ? `not in top ${LIMIT}` : `#${i + 1}`;
}

async function waitBufferDrained() {
  for (let i = 0; i < 40; i++) {
    const m = (await (await fetch(`${BASE}/metrics`)).json()) as { total?: { buffered?: number } };
    if ((m.total?.buffered ?? 0) === 0) return;
    await Bun.sleep(250);
  }
}

async function main() {
  console.log(`# Recency demo  prefix="${PREFIX}"  hot query="${HOT}"  burst=${BURST}\n`);

  console.log(`BEFORE -- ${HOT} is ${rankOf(await suggest("basic"))} by count, ${rankOf(await suggest("recency"))} by recency`);
  console.log(table(await suggest("basic"), await suggest("recency")));

  // Burst: many searches for the hot query. It routes by hash(query); the
  // updater bridges it to the prefix's shard cache.
  console.log(`\n... submitting ${BURST} searches for "${HOT}"`);
  const workers = Array.from({ length: 32 }, async () => {
    for (let i = 0; i < Math.ceil(BURST / 32); i++) {
      await fetch(`${BASE}/search`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: HOT }),
      });
    }
  });
  await Promise.all(workers);

  await waitBufferDrained();
  // Give the cache-updater a few poll cycles to rebuild the prefix's caches.
  let after: string[] = [];
  for (let i = 0; i < 20; i++) {
    await Bun.sleep(500);
    after = await suggest("recency");
    if (after.includes(HOT)) break;
  }

  console.log(`\nAFTER  -- ${HOT} is ${rankOf(await suggest("basic"))} by count, ${rankOf(await suggest("recency"))} by recency`);
  console.log(table(await suggest("basic"), after));
  console.log(
    `\n-> The same /suggest API returns a recency-aware order under rank=recency: a ` +
      `query that was\n  ${rankOf(await suggest("basic"))} by all-time count is ${rankOf(after)} once it is active.`,
  );
}

main();
