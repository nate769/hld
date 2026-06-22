/**
 * Load balancer / reverse proxy.
 *
 * The single public entrypoint. Serves the frontend and proxies API calls to
 * the correct app node using the same consistent-hash ring the seeder used,
 * so a request for prefix "goo" always lands on the node whose shard holds
 * `q:goo`. Every routing decision is logged.
 *
 *   GET  /              -> static frontend
 *   GET  /suggest?q=p   -> hash(p) -> proxy to that app node
 *   POST /search {q}    -> hash(q) -> proxy to that app node
 *   GET  /trending      -> fan-out to all app nodes, merge, return global top-N
 *   GET  /metrics       -> fan-out to all app nodes, sum, return aggregate
 *
 * `/trending` is fan-out + merge because the trending ZSET is sharded: each
 * query lives on exactly one shard, so merging per-shard top-N and re-sorting
 * yields the correct global ranking.
 */

import { route } from "./hash-ring";
import {
  SHARDS,
  TRENDING_LIMIT,
  appUrlFor,
  normalize,
  type ShardId,
} from "./config";

const PORT = Number(process.env.LB_PORT ?? process.env.PORT ?? 8080);
const MAX_BODY_BYTES = 10 * 1024;
const MAX_KEY_LEN = 100;
const MAX_TRENDING_LIMIT = 100;
const STATIC_DIR = "public";

interface ScoredQuery {
  query: string;
  score: number;
}

interface TrendingResponse {
  trending?: ScoredQuery[];
}

// ---------------------------------------------------------------------------
// Logging + helpers
// ---------------------------------------------------------------------------

function logRoute(method: string, path: string, key: string, shard: ShardId): void {
  console.log(`[LB] ${method} ${path} key="${key}" -> app${shard}`);
}

const errorResponse = (msg: string, status: number) =>
  Response.json({ error: msg }, { status });

/** Forward a request to the app node that owns `shard`. */
async function proxy(shard: ShardId, path: string, init?: RequestInit): Promise<Response> {
  try {
    return await fetch(appUrlFor(shard) + path, init);
  } catch (err) {
    console.error(`[LB] upstream app${shard} unreachable:`, (err as Error).message);
    return errorResponse(`app node ${shard} unavailable`, 502);
  }
}

/** Run an async fn against every shard, dropping failures. */
async function fanOut<T>(fn: (shard: ShardId) => Promise<T>): Promise<T[]> {
  const settled = await Promise.allSettled(SHARDS.map(fn));
  return settled.flatMap((r) => (r.status === "fulfilled" ? [r.value] : []));
}

function contentLengthOf(req: Request): number {
  return Number(req.headers.get("content-length") ?? 0);
}

// ---------------------------------------------------------------------------
// Fan-out endpoints
// ---------------------------------------------------------------------------

async function mergedTrending(limit: number): Promise<Response> {
  const responses = await fanOut(
    (shard) =>
      fetch(`${appUrlFor(shard)}/trending?limit=${limit}`).then(
        (r) => r.json() as Promise<TrendingResponse>,
      ),
  );

  // Each query lives on exactly one shard, so a flat sort is the correct merge.
  const merged: ScoredQuery[] = responses.flatMap((r) => r.trending ?? []);
  merged.sort((a, b) => b.score - a.score);

  return Response.json({ trending: merged.slice(0, limit) });
}

async function aggregatedMetrics(): Promise<Response> {
  const nodes = await fanOut(
    (shard) => fetch(`${appUrlFor(shard)}/metrics`).then((r) => r.json() as Promise<Record<string, number>>),
  );

  const total = {
    searchesReceived: 0,
    batchesFlushed: 0,
    rowsUpserted: 0,
    cacheHits: 0,
    cacheMisses: 0,
    buffered: 0,
  };
  for (const node of nodes) {
    for (const key of Object.keys(total) as (keyof typeof total)[]) {
      total[key] += node[key] ?? 0;
    }
  }

  const suggestTotal = total.cacheHits + total.cacheMisses;
  return Response.json({
    total: {
      ...total,
      cacheHitRate: suggestTotal > 0 ? total.cacheHits / suggestTotal : null,
      writeReduction:
        total.batchesFlushed > 0 ? total.searchesReceived / total.batchesFlushed : null,
    },
    nodes,
  });
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

function handleSuggest(url: URL): Promise<Response> | Response {
  const raw = normalize(url.searchParams.get("q") ?? "");
  const key = raw.slice(0, MAX_KEY_LEN);
  if (!key) return Response.json({ prefix: "", suggestions: [] });

  const shard = route(key);
  logRoute("GET", "/suggest", key, shard);

  const params = new URLSearchParams(url.searchParams);
  params.set("q", key);
  return proxy(shard, `/suggest?${params.toString()}`);
}

async function handleSearch(req: Request): Promise<Response> {
  if (contentLengthOf(req) > MAX_BODY_BYTES) {
    return errorResponse("request entity too large", 413);
  }

  const body = await req.text();
  let key: string;
  try {
    key = normalize((JSON.parse(body) as { query?: string }).query ?? "");
  } catch {
    return errorResponse("invalid json body", 400);
  }
  if (!key) return errorResponse("missing query", 400);

  const shard = route(key);
  logRoute("POST", "/search", key, shard);
  return proxy(shard, "/search", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });
}

function handleTrending(url: URL): Promise<Response> {
  const raw = Number(url.searchParams.get("limit"));
  const parsed = Number.isInteger(raw) && raw > 0 ? raw : TRENDING_LIMIT;
  return mergedTrending(Math.min(parsed, MAX_TRENDING_LIMIT));
}

function handleCacheDebug(url: URL): Promise<Response> | Response {
  const key = normalize(url.searchParams.get("prefix") ?? "");
  if (!key) return errorResponse("missing prefix", 400);
  const shard = route(key);
  logRoute("GET", "/cache/debug", key, shard);
  return proxy(shard, `/cache/debug${url.search}`);
}

// ---------------------------------------------------------------------------
// Static asset serving
// ---------------------------------------------------------------------------

const STATIC_FILES: Record<string, string> = {
  "/": `${STATIC_DIR}/index.html`,
  "/index.html": `${STATIC_DIR}/index.html`,
  "/script.js": `${STATIC_DIR}/script.js`,
};

function serveStatic(pathname: string): Response | null {
  const file = STATIC_FILES[pathname];
  if (file) return new Response(Bun.file(file));
  if (pathname === "/favicon.ico") return new Response(null, { status: 204 });
  return null;
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
    if (method === "GET" && pathname === "/metrics") return aggregatedMetrics();
    if (method === "GET" && pathname === "/cache/debug") return handleCacheDebug(url);

    if (method === "GET") {
      const asset = serveStatic(pathname);
      if (asset) return asset;
    }

    return new Response("Not Found", { status: 404 });
  },
});

console.log(`Load balancer on :${server.port}`);
for (const shard of SHARDS) console.log(`     app${shard} -> ${appUrlFor(shard)}`);
