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
import {
  entityKeySql,
  entityScopeSql,
  type EntityGrain,
  type EntityScope,
} from "./entity-scope";

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

type SentTotals = { toLeads: number; manualReplies: number; warmup: number; warmupReplies: number; seeds: number };

export interface SentPerPeriod {
  grain: SentGrain;
  timezone: "UTC";
  since: string | null;
  asOf: string;
  /** The account / mailbox / domain filter applied (empty = the fleet). */
  scope: EntityScope;
  totals: SentTotals;
  periods: SentPeriod[];
  /**
   * Present only with `groupBy`: the same periods per entity (account, mailbox
   * or domain), every period emitted, only entities that sent at least once in
   * the window. Their per-period sums equal `periods` (one mailbox never spans
   * two domains); `leadsEmailed` is distinct per entity and does not add.
   */
  groupBy?: EntityGrain;
  entities?: { key: string; totals: SentTotals; periods: SentPeriod[] }[];
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
function sumTotals(periods: SentPeriod[]): SentTotals {
  const totals = { toLeads: 0, manualReplies: 0, warmup: 0, warmupReplies: 0, seeds: 0 };
  for (const p of periods) {
    totals.toLeads += p.toLeads;
    totals.manualReplies += p.manualReplies;
    totals.warmup += p.warmup;
    totals.warmupReplies += p.warmupReplies;
    totals.seeds += p.seeds;
  }
  return totals;
}

function mapPeriodRow(r: Record<string, unknown>): SentPeriod {
  return {
    periodStart: String(r.period_start),
    periodEnd: String(r.period_end),
    inProgress: r.in_progress === true || r.in_progress === "t" || r.in_progress === "true",
    toLeads: int(r.to_leads),
    manualReplies: int(r.manual_replies),
    warmup: int(r.warmup),
    warmupReplies: int(r.warmup_replies),
    seeds: int(r.seeds),
    leadsEmailed: int(r.leads_emailed),
  };
}

export function mapSentPeriods(
  rows: Record<string, unknown>[],
  meta: { grain: SentGrain; since: string | null; asOf: string; scope?: EntityScope },
): SentPerPeriod {
  const periods = rows.map(mapPeriodRow);
  return {
    grain: meta.grain,
    timezone: "UTC",
    since: meta.since,
    asOf: meta.asOf,
    scope: meta.scope ?? {},
    totals: sumTotals(periods),
    periods,
  };
}

/**
 * Pure: spread per-(entity, period) count rows over the window's periods, zeros
 * filled, entities sorted by key.
 */
export function mapEntityPeriods(
  periods: SentPeriod[],
  rows: Record<string, unknown>[],
): { key: string; totals: SentTotals; periods: SentPeriod[] }[] {
  const byKey = new Map<string, Map<string, SentPeriod>>();
  for (const r of rows) {
    const key = String(r.entity_key);
    const p = mapPeriodRow(r);
    const m = byKey.get(key) ?? new Map<string, SentPeriod>();
    m.set(p.periodStart, p);
    byKey.set(key, m);
  }
  return [...byKey.keys()].sort().map((key) => {
    const m = byKey.get(key)!;
    const series = periods.map(
      (p) =>
        m.get(p.periodStart) ?? {
          periodStart: p.periodStart,
          periodEnd: p.periodEnd,
          inProgress: p.inProgress,
          toLeads: 0,
          manualReplies: 0,
          warmup: 0,
          warmupReplies: 0,
          seeds: 0,
          leadsEmailed: 0,
        },
    ).map((p, i) => ({ ...p, periodEnd: periods[i].periodEnd, inProgress: periods[i].inProgress }));
    return { key, totals: sumTotals(series), periods: series };
  });
}

export async function readSentPerPeriod(opts: {
  grain: SentGrain;
  since: string | null;
  scope?: EntityScope;
  groupBy?: EntityGrain | null;
}): Promise<SentPerPeriod> {
  if (!SENT_GRAINS.includes(opts.grain)) throw new Error(`unknown grain ${opts.grain}`);
  // Whitelisted above: the grain is one of three literals, safe to inline.
  const unit = sql.raw(`'${opts.grain}'`);
  const step = sql.raw(`interval '1 ${opts.grain}'`);
  const kinds = sql.raw(SENT_KINDS.map((k) => `'${k}'`).join(","));
  const since = opts.since === null ? sql`NULL::timestamptz` : sql`${opts.since}::timestamptz`;
  const scope = opts.scope ?? {};
  const scoped = entityScopeSql(scope, sql`m.account_email`, sql`m.mailbox_login`);

  const result = await db.execute(sql`
    WITH bounds AS (
      SELECT
        date_trunc(${unit}, COALESCE(${since}, (
          SELECT min(m.occurred_at) FROM messages m
          WHERE m.direction = 'out' AND m.outcome = 'sent' AND m.kind IN (${kinds}) AND ${scoped}
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
        AND m.occurred_at >= (b.start AT TIME ZONE 'UTC') AND ${scoped}
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

  const served = mapSentPeriods(rowsOf<Record<string, unknown>>(result), {
    grain: opts.grain,
    since: opts.since,
    asOf: new Date().toISOString(),
    scope,
  });
  if (!opts.groupBy || served.periods.length === 0) return served;

  const key = entityKeySql(opts.groupBy, sql`m.account_email`, sql`m.mailbox_login`);
  const first = served.periods[0].periodStart;
  const grouped = await db.execute(sql`
    SELECT
      ${key} AS entity_key,
      to_char(date_trunc(${unit}, m.occurred_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD') AS period_start,
      count(*) FILTER (WHERE m.kind = 'outreach')::int AS to_leads,
      count(*) FILTER (WHERE m.kind = 'manual_reply')::int AS manual_replies,
      count(*) FILTER (WHERE m.kind = 'warmup')::int AS warmup,
      count(*) FILTER (WHERE m.kind = 'warmup_reply')::int AS warmup_replies,
      count(*) FILTER (WHERE m.kind = 'seed')::int AS seeds,
      count(DISTINCT lower(m.counterparty)) FILTER (WHERE m.kind IN ('outreach', 'manual_reply'))::int AS leads_emailed
    FROM messages m
    WHERE m.direction = 'out' AND m.outcome = 'sent' AND m.kind IN (${kinds})
      AND m.occurred_at >= (${first}::timestamp AT TIME ZONE 'UTC') AND ${scoped}
    GROUP BY 1, 2
  `);
  return {
    ...served,
    groupBy: opts.groupBy,
    entities: mapEntityPeriods(served.periods, rowsOf<Record<string, unknown>>(grouped)),
  };
}
