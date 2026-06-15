import net from "node:net";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import * as dotenv from "dotenv";
import { withConnectRetry } from "./retry";

dotenv.config();

// Neon scale-to-zero parks the compute after ~5 min idle; the first connection
// after that triggers a cold resume that can take several seconds. Node 20's
// happy-eyeballs gives each candidate address only 250ms, so the first query
// after idle fails with AggregateError [ETIMEDOUT] before the compute wakes.
// Widen the per-address attempt window to cover a cold resume. Set at module
// load — BEFORE the pool is created and before migrate() runs at boot — so the
// boot migration (CREATE SCHEMA) connect is covered too.
net.setDefaultAutoSelectFamilyAttemptTimeout(5000);

function enforceSslMode(url: string | undefined): string | undefined {
  if (!url) return url;
  if (url.includes("sslmode=")) {
    return url.replace(/sslmode=[^&]+/, "sslmode=verify-full");
  }
  return `${url}${url.includes("?") ? "&" : "?"}sslmode=verify-full`;
}

const pool = new Pool({
  connectionString: enforceSslMode(process.env.INSTANTLY_SERVICE_DATABASE_URL),
  // Bound the connect wait so a genuinely dead host fails loud instead of
  // hanging forever — the post-TCP startup phase is not covered by the
  // happy-eyeballs attempt timeout above. Under a stats burst the pool can
  // saturate (all `max` connections busy on a slow aggregation); the 15s
  // default left the next acquire BLOCKED for 15s before withConnectRetry even
  // retried, stacking latency. 5s fails fast → retry, while still covering a
  // staging/dev cold Neon resume (1-7s) on the first attempt.
  connectionTimeoutMillis: 5_000,
  // Analytics / status routes fan out bursts of concurrent reads. The pg
  // default max of 10 saturates under a batch caller, so the 11th acquire
  // waits out connectionTimeoutMillis and throws "timeout exceeded when trying
  // to connect" (a 500 on /orgs/status + /orgs/analytics). Give the pool more
  // headroom — Neon's compute allows 450 connections.
  max: 20,
  // The Neon compute lives in ap-southeast-1; opening a fresh connection pays a
  // multi-round-trip cross-region TLS handshake. The pg default idle timeout
  // (10s) tears warm connections down between bursts, forcing a slow reconnect
  // on the next one and amplifying acquire timeouts. Keep idle connections warm
  // and enable TCP keepalive so they survive between bursts.
  idleTimeoutMillis: 60_000,
  keepAlive: true,
});

// Retry only connection-ACQUISITION failures (cold Neon resume). drizzle runs
// every statement via pool.query (no transactions / pool.connect in this repo),
// so wrapping pool.query is the single chokepoint covering all db.* calls AND
// the boot migrator's CREATE SCHEMA / migration-table pre-checks. The query has
// not been dispatched when these errors fire, so the retry is safe for writes too.
/* eslint-disable @typescript-eslint/no-explicit-any */
const baseQuery = pool.query.bind(pool) as (...args: any[]) => any;
pool.query = function retryingQuery(...args: any[]): any {
  // pg's callback form (last arg is a function) is never used by drizzle; only
  // the promise form is retryable.
  if (typeof args[args.length - 1] === "function") {
    return baseQuery(...args);
  }
  return withConnectRetry(() => baseQuery(...args), {
    onRetry: (attempt, delayMs, err) => {
      const detail = (err as { code?: string }).code ?? (err as Error)?.message;
      console.warn(
        `[instantly-service] DB connection failed (attempt ${attempt}), retrying in ${delayMs}ms: ${detail}`,
      );
    },
  });
} as typeof pool.query;
/* eslint-enable @typescript-eslint/no-explicit-any */

// The gold stats aggregations (analytics.ts / status.ts) run
// COUNT(DISTINCT lead_email) over ~88k events; at the pg default work_mem (4MB)
// that sort spills to temp disk (EXPLAIN: "external merge  Disk: 7616kB"),
// adding I/O contention and holding the pool connection longer under burst.
// Raise work_mem per session so the sort stays in memory — faster scans,
// connections released sooner. Safe on RAM: the compute autoscales to 1 CU /
// 4GB precisely under this load, and idle (0.25 CU floor) runs no such sorts.
pool.on("connect", (client) => {
  void client.query("SET work_mem = '32MB'");
});

export const db = drizzle(pool);

export async function closeDb() {
  await pool.end();
}
