/**
 * The one foreign-exchange rate the infrastructure spend needs: EUR → USD.
 *
 * Gandi bills in euros and every other vendor in dollars, so any single figure
 * for "what the estate costs" needs a rate. It used to be refused outright on
 * the grounds that this service owned none, and the refusal left a worse
 * outcome in place: the staff page summed `monthlyCents` across every row and
 * labelled the result USD, i.e. it added euro cents to dollar cents. A real
 * rate, stored with where it came from and which day it is for, is the honest
 * version of that sum.
 *
 * Source: the European Central Bank's daily reference rates — free, keyless,
 * published once per TARGET business day. Stored append-only, one row per
 * reference day, the same non-retroactive posture as `infra_price_rates`.
 *
 * ⚠️ There is NO fallback rate anywhere. With nothing on record every USD twin
 * reads null and the consumer says the blend is unavailable. A guessed rate
 * would become the denominator of every cost-per-email on the page.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";

export const ECB_DAILY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml";
export const ECB_SOURCE = "ecb-eurofxref-daily";

export interface FxRate {
  base: "EUR";
  quote: "USD";
  /** Units of USD for one EUR. */
  rate: number;
  /** The ECB reference day this rate is FOR (YYYY-MM-DD), not when we fetched it. */
  asOf: string;
  source: string;
}

/**
 * Read the reference day and the USD rate out of the ECB daily envelope.
 * Throws on anything it cannot read: a half-parsed rate is worse than none.
 */
export function parseEcbDailyXml(xml: string): { asOf: string; eurUsd: number } {
  const day = /<Cube\s+time=['"](\d{4}-\d{2}-\d{2})['"]/.exec(xml)?.[1];
  const usd = /<Cube\s+currency=['"]USD['"]\s+rate=['"]([0-9.]+)['"]/.exec(xml)?.[1];
  if (!day) throw new Error("ECB envelope carries no reference day");
  if (!usd) throw new Error("ECB envelope carries no USD rate");
  const eurUsd = Number(usd);
  if (!Number.isFinite(eurUsd) || eurUsd <= 0) throw new Error(`ECB USD rate is not a positive number: ${usd}`);
  return { asOf: day, eurUsd };
}

/**
 * A native amount in USD cents. USD passes through; EUR converts at the rate;
 * anything else — an unknown currency, no rate on record, no amount — is null.
 * Null is an answer here: it says "we cannot state this in dollars".
 */
export function toUsdCents(cents: number | null, currency: string | null, fx: FxRate | null): number | null {
  if (cents === null || currency === null) return null;
  if (currency === "USD") return cents;
  if (currency === "EUR" && fx) return Math.round(cents * fx.rate);
  return null;
}

/** The rate as it travels on a response, so every USD figure is traceable to its day and source. */
export function describeFx(fx: FxRate | null) {
  return fx ? { base: fx.base, quote: fx.quote, rate: fx.rate, asOf: fx.asOf, source: fx.source } : null;
}

export async function fetchEcbEurUsd(fetchImpl: typeof fetch = fetch): Promise<{ asOf: string; eurUsd: number }> {
  const res = await fetchImpl(ECB_DAILY_URL);
  if (!res.ok) throw new Error(`ECB reference rates answered ${res.status}`);
  return parseEcbDailyXml(await res.text());
}

/**
 * Fetch today's reference rate and record it. Idempotent: a second run on the
 * same reference day inserts nothing. Fails loud — the caller logs it.
 */
export async function syncFxRates(fetchImpl: typeof fetch = fetch) {
  const { asOf, eurUsd } = await fetchEcbEurUsd(fetchImpl);
  const result = await db.execute(sql`
    INSERT INTO fx_rates (base, quote, rate, as_of, source)
    VALUES ('EUR', 'USD', ${eurUsd}, ${asOf}, ${ECB_SOURCE})
    ON CONFLICT (base, quote, as_of) DO NOTHING
  `);
  const inserted = (result as { rowCount?: number | null }).rowCount ?? 0;
  return { asOf, eurUsd, inserted };
}

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  return ((result as { rows?: T[] }).rows ?? []) as T[];
}

/** The most recent EUR → USD rate on record, or null when none has ever been fetched. */
export async function loadLatestEurUsd(): Promise<FxRate | null> {
  const result = await db.execute(sql`
    SELECT rate, to_char(as_of, 'YYYY-MM-DD') AS as_of, source
      FROM fx_rates
     WHERE base = 'EUR' AND quote = 'USD'
     ORDER BY as_of DESC
     LIMIT 1
  `);
  const row = rowsOf<{ rate: string | number; as_of: string; source: string }>(result)[0];
  if (!row) return null;
  return { base: "EUR", quote: "USD", rate: Number(row.rate), asOf: String(row.as_of), source: String(row.source) };
}
