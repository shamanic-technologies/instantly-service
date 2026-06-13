/**
 * Connection-acquisition retry for Neon scale-to-zero cold starts.
 *
 * When the Neon compute is suspended (scale-to-zero), the first connection after
 * idle hits a compute that is still resuming. Node 20's happy-eyeballs gives each
 * candidate address only 250ms, so the connect fails with an AggregateError
 * [ETIMEDOUT] before the compute finishes waking. These failures occur BEFORE any
 * statement is dispatched, so re-running the query is safe for reads and writes
 * alike — the query never executed.
 */

const TRANSIENT_CONNECT_CODES = new Set([
  "ETIMEDOUT",
  "ECONNREFUSED",
  "ECONNRESET",
  "EHOSTUNREACH",
  "ENETUNREACH",
]);

/**
 * True only for errors raised while ACQUIRING a connection (pre-dispatch), which
 * are safe to retry. Covers:
 *  - Node happy-eyeballs AggregateError (its own `.code` is `ETIMEDOUT`)
 *  - raw socket errors while the Neon proxy is still waking the compute
 *  - pg Client-level connect timeout ("timeout expired")
 *  - pg Pool-level acquire timeout ("timeout exceeded when trying to connect"):
 *    the pool could not hand out a connection within `connectionTimeoutMillis`,
 *    either because a fresh connect (cross-region to Neon) ran long or the pool
 *    was momentarily saturated. The query never dispatched, so retry is safe.
 *
 * Returns false for SQL errors and statement timeouts (57014) — by then the
 * statement has run, so retrying could double-apply a write.
 */
export function isTransientConnectError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const code = (err as { code?: unknown }).code;
  if (typeof code === "string" && TRANSIENT_CONNECT_CODES.has(code)) return true;
  const message = (err as { message?: unknown }).message;
  return (
    typeof message === "string" &&
    /timeout expired|timeout exceeded when trying to connect/i.test(message)
  );
}

export interface ConnectRetryOptions {
  /** Number of retries after the initial attempt. Default 3. */
  retries?: number;
  /** Base backoff in ms; doubles each retry. Default 250. */
  baseDelayMs?: number;
  /** Injected for tests; defaults to setTimeout-based sleep. */
  sleep?: (ms: number) => Promise<void>;
  /** Called before each backoff sleep. */
  onRetry?: (attempt: number, delayMs: number, err: unknown) => void;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Run `fn`, retrying only transient connection-acquisition failures with
 * exponential backoff. Non-transient errors propagate immediately.
 */
export async function withConnectRetry<T>(
  fn: () => Promise<T>,
  options: ConnectRetryOptions = {},
): Promise<T> {
  const retries = options.retries ?? 3;
  const baseDelayMs = options.baseDelayMs ?? 250;
  const sleep = options.sleep ?? defaultSleep;

  let attempt = 0;
  for (;;) {
    try {
      return await fn();
    } catch (err) {
      if (attempt >= retries || !isTransientConnectError(err)) throw err;
      const delayMs = baseDelayMs * 2 ** attempt;
      attempt += 1;
      options.onRetry?.(attempt, delayMs, err);
      await sleep(delayMs);
    }
  }
}
