/**
 * Emails SENT per period, split by what each one was for (staff ops read).
 *
 * Source: the `messages` projection, outbound rows whose outcome is `sent`
 * (a bounce at hand-off — `permanent` / `transient` — never left us, so it is
 * not counted). One bucket per purpose:
 *   - toLeads        `outreach`      cold sequence steps to a lead, both transports
 *   - manualReplies  `manual_reply`  a human's answer to a lead (reaches a lead, but
 *                                    is not cold outreach, so it is kept apart from
 *                                    `toLeads`)
 *   - warmup         `warmup`        our own warmup mesh
 *   - warmupReplies  `warmup_reply`  replies inside the warmup mesh
 *   - seeds          `seed`          inbox-placement / seed tests
 * `leadsEmailed` = distinct lead addresses (lower-cased) that received an
 * `outreach` or `manual_reply` in the period; it does not add across periods.
 *
 * Periods are UTC calendar buckets (`day`, ISO `week` starting Monday, `month`),
 * every one from the first send (or `since`) to the current one, zeros included.
 * The current bucket is flagged `inProgress` here so no reader needs a clock.
 */

import { sql } from "drizzle-orm";
import { db } from "../../db";

export const SENT_GRAINS = ["day", "week", "month"] as const;
export type SentGrain = (typeof SENT_GRAINS)[number];

export const SENT_KINDS = ["outreach", "manual_reply", "warmup", "warmup_reply", "seed"] as const;

export interface SentPeriod {
  periodStart: string;
  periodEnd: string;
  inProgress: boolean;
  toLeads: number;
  manualReplies: number;
  warmup: number;
  warmupReplies: number;
  seeds: number;
  leadsEmailed: number;
}

export interface SentPerPeriod {
  grain: SentGrain;
  timezone: "UTC";
  since: string | null;
  asOf: string;
  totals: { toLeads: number; manualReplies: number; warmup: number; warmupReplies: number; seeds: number };
  periods: SentPeriod[];
}

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

function int(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

/** Pure: map the SQL rows (period text + counts, possibly strings) to the served shape. */
export function mapSentPeriods(
  rows: Record<string, unknown>[],
  meta: { grain: SentGrain; since: string | null; asOf: string },
): SentPerPeriod {
  const periods: SentPeriod[] = rows.map((r) => ({
    periodStart: String(r.period_start),
    periodEnd: String(r.period_end),
    inProgress: r.in_progress === true || r.in_progress === "t" || r.in_progress === "true",
    toLeads: int(r.to_leads),
    manualReplies: int(r.manual_replies),
    warmup: int(r.warmup),
    warmupReplies: int(r.warmup_replies),
    seeds: int(r.seeds),
    leadsEmailed: int(r.leads_emailed),
  }));
  const totals = { toLeads: 0, manualReplies: 0, warmup: 0, warmupReplies: 0, seeds: 0 };
  for (const p of periods) {
    totals.toLeads += p.toLeads;
    totals.manualReplies += p.manualReplies;
    totals.warmup += p.warmup;
    totals.warmupReplies += p.warmupReplies;
    totals.seeds += p.seeds;
  }
  return { grain: meta.grain, timezone: "UTC", since: meta.since, asOf: meta.asOf, totals, periods };
}

export async function readSentPerPeriod(opts: { grain: SentGrain; since: string | null }): Promise<SentPerPeriod> {
  if (!SENT_GRAINS.includes(opts.grain)) throw new Error(`unknown grain ${opts.grain}`);
  // Whitelisted above: the grain is one of three literals, safe to inline.
  const unit = sql.raw(`'${opts.grain}'`);
  const step = sql.raw(`interval '1 ${opts.grain}'`);
  const kinds = sql.raw(SENT_KINDS.map((k) => `'${k}'`).join(","));
  const since = opts.since === null ? sql`NULL::timestamptz` : sql`${opts.since}::timestamptz`;

  const result = await db.execute(sql`
    WITH bounds AS (
      SELECT
        date_trunc(${unit}, COALESCE(${since}, (
          SELECT min(occurred_at) FROM messages
          WHERE direction = 'out' AND outcome = 'sent' AND kind IN (${kinds})
        )) AT TIME ZONE 'UTC') AS start,
        date_trunc(${unit}, now() AT TIME ZONE 'UTC') AS current
    ),
    periods AS (
      SELECT generate_series(b.start, b.current, ${step}) AS period_start
      FROM bounds b WHERE b.start IS NOT NULL
    ),
    counts AS (
      SELECT
        date_trunc(${unit}, m.occurred_at AT TIME ZONE 'UTC') AS period_start,
        count(*) FILTER (WHERE m.kind = 'outreach') AS to_leads,
        count(*) FILTER (WHERE m.kind = 'manual_reply') AS manual_replies,
        count(*) FILTER (WHERE m.kind = 'warmup') AS warmup,
        count(*) FILTER (WHERE m.kind = 'warmup_reply') AS warmup_replies,
        count(*) FILTER (WHERE m.kind = 'seed') AS seeds,
        count(DISTINCT lower(m.counterparty)) FILTER (WHERE m.kind IN ('outreach', 'manual_reply')) AS leads_emailed
      FROM messages m, bounds b
      WHERE m.direction = 'out' AND m.outcome = 'sent' AND m.kind IN (${kinds})
        AND m.occurred_at >= (b.start AT TIME ZONE 'UTC')
      GROUP BY 1
    )
    SELECT
      to_char(p.period_start, 'YYYY-MM-DD') AS period_start,
      to_char(p.period_start + ${step}, 'YYYY-MM-DD') AS period_end,
      p.period_start = (SELECT current FROM bounds) AS in_progress,
      COALESCE(c.to_leads, 0)::int AS to_leads,
      COALESCE(c.manual_replies, 0)::int AS manual_replies,
      COALESCE(c.warmup, 0)::int AS warmup,
      COALESCE(c.warmup_replies, 0)::int AS warmup_replies,
      COALESCE(c.seeds, 0)::int AS seeds,
      COALESCE(c.leads_emailed, 0)::int AS leads_emailed
    FROM periods p
    LEFT JOIN counts c USING (period_start)
    ORDER BY p.period_start
  `);

  return mapSentPeriods(rowsOf<Record<string, unknown>>(result), {
    grain: opts.grain,
    since: opts.since,
    asOf: new Date().toISOString(),
  });
}
