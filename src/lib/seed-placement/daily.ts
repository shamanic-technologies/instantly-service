/**
 * The stored daily series of seed placement, and its staff read.
 * MS-SEED-PLACEMENT-1018.
 *
 * One row per (UTC day the seeds left, receiving provider, sending vendor):
 * seeds sent, found in Inbox (Focused / Other split for Microsoft), in spam, and
 * never found. Recomputed from the two bronze tables on every seed sync, so a
 * seed read a day late moves out of `missing` on the row it belongs to.
 *
 * The SENDING VENDOR is kept because it is most of the story: since 2026-09-19
 * the legacy Gandi shared-IP mailboxes (which send ~5 % of prospect mail) write
 * ~2/3 of the seeds and land 87-98 % in Gmail spam, while the Primeforge
 * mailboxes that send the rest land 0-13 % in spam. A fleet-wide figure alone
 * reads like the prospects' mail is half spam; it is not.
 *
 * Read by `GET /internal/seed-placement/daily` (the admin chart: Google inbox %
 * and Microsoft inbox % per day).
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { ESP_GOOGLE, ESP_OUTLOOK, ESP_OUTLOOK_CONSUMER } from "./seeds";

function rowsOf<T>(result: unknown): T[] {
  return ((result as { rows?: T[] }).rows ?? []) as T[];
}

/** Recompute every day of the series from bronze. Returns the rows written. */
export async function refreshSeedPlacementDaily(): Promise<number> {
  // A row whose (day, ESP, vendor) no longer exists in bronze (a receiver
  // relabelled from ESP 2 to 4, SENDER-HEALTH-1019) is dropped, never left stale.
  await db.execute(sql`
    DELETE FROM seed_placement_daily s
    WHERE NOT EXISTS (
      SELECT 1 FROM seed_placement_dispatches d
      WHERE d.outcome = 'sent' AND d.recipient_esp = s.recipient_esp
        AND (d.dispatched_at AT TIME ZONE 'UTC')::date = s.day
    )
  `);
  const result = await db.execute(sql`
    INSERT INTO seed_placement_daily
      (day, recipient_esp, sender_vendor, seeds, inbox, spam, missing, inbox_focused, inbox_other, updated_at)
    SELECT
      (d.dispatched_at AT TIME ZONE 'UTC')::date,
      d.recipient_esp,
      COALESCE(v.provider, 'unknown'),
      count(*)::int,
      count(o.id) FILTER (WHERE o.placement = 'inbox')::int,
      count(o.id) FILTER (WHERE o.placement = 'spam')::int,
      (count(*) - count(o.id))::int,
      count(o.id) FILTER (WHERE o.placement = 'inbox' AND o.ms_inference = 'focused')::int,
      count(o.id) FILTER (WHERE o.placement = 'inbox' AND o.ms_inference = 'other')::int,
      now()
    FROM seed_placement_dispatches d
    LEFT JOIN seed_placement_observations o
      ON o.message_id = d.message_id AND o.receiver_email = d.receiver_email
    -- Two vendors can report one domain; the one that registered it first owns it.
    LEFT JOIN LATERAL (
      SELECT i.provider FROM infra_domains i
      WHERE i.domain = lower(split_part(d.sender_email, '@', 2))
      ORDER BY i.created_at_provider ASC NULLS LAST, i.provider ASC
      LIMIT 1
    ) v ON true
    WHERE d.outcome = 'sent'
    GROUP BY 1, 2, 3
    ON CONFLICT (day, recipient_esp, sender_vendor) DO UPDATE SET
      seeds = EXCLUDED.seeds, inbox = EXCLUDED.inbox, spam = EXCLUDED.spam, missing = EXCLUDED.missing,
      inbox_focused = EXCLUDED.inbox_focused, inbox_other = EXCLUDED.inbox_other, updated_at = EXCLUDED.updated_at
  `);
  return (result as { rowCount?: number | null }).rowCount ?? 0;
}

export interface PlacementCounts {
  seeds: number;
  inbox: number;
  spam: number;
  missing: number;
  /** inbox / seeds, 0..100 rounded to 1 decimal; null on a day with no seed. */
  inboxPct: number | null;
  spamPct: number | null;
}

export interface MicrosoftPlacementCounts extends PlacementCounts {
  inboxFocused: number;
  inboxOther: number;
}

export interface SeedPlacementDay {
  day: string;
  google: PlacementCounts;
  /** A Microsoft 365 business tenant (ESP 2). */
  microsoft: MicrosoftPlacementCounts;
  /** Outlook.com consumer (ESP 4): filters apart from Microsoft 365 (SENDER-HEALTH-1019). */
  microsoftConsumer: MicrosoftPlacementCounts;
  /** Same split per sending vendor (gandi, primeforge, ...). */
  byVendor: {
    vendor: string;
    google: PlacementCounts;
    microsoft: MicrosoftPlacementCounts;
    microsoftConsumer: MicrosoftPlacementCounts;
  }[];
}

export interface DailyRow {
  day: string;
  recipientEsp: number;
  senderVendor: string;
  seeds: number;
  inbox: number;
  spam: number;
  missing: number;
  inboxFocused: number;
  inboxOther: number;
}

const pct = (n: number, d: number): number | null => (d > 0 ? Math.round((n * 1000) / d) / 10 : null);

function emptyMs(): MicrosoftPlacementCounts {
  return { seeds: 0, inbox: 0, spam: 0, missing: 0, inboxPct: null, spamPct: null, inboxFocused: 0, inboxOther: 0 };
}

function add(target: MicrosoftPlacementCounts, row: DailyRow): void {
  target.seeds += row.seeds;
  target.inbox += row.inbox;
  target.spam += row.spam;
  target.missing += row.missing;
  target.inboxFocused += row.inboxFocused;
  target.inboxOther += row.inboxOther;
}

function finish(c: MicrosoftPlacementCounts): MicrosoftPlacementCounts {
  return { ...c, inboxPct: pct(c.inbox, c.seeds), spamPct: pct(c.spam, c.seeds) };
}

function googleOnly(c: MicrosoftPlacementCounts): PlacementCounts {
  const { inboxFocused: _f, inboxOther: _o, ...rest } = finish(c);
  return rest;
}

/**
 * Shape stored rows into one entry per UTC day of the window, EVERY day emitted
 * (a day with no test has zero seeds and null percentages, never a fake 0 %). Pure.
 */
export function buildSeedPlacementDays(rows: readonly DailyRow[], fromDay: string, toDay: string): SeedPlacementDay[] {
  const days: SeedPlacementDay[] = [];
  const byDay = new Map<string, DailyRow[]>();
  for (const r of rows) byDay.set(r.day, [...(byDay.get(r.day) ?? []), r]);

  for (let t = Date.parse(`${fromDay}T00:00:00Z`); t <= Date.parse(`${toDay}T00:00:00Z`); t += 86_400_000) {
    const day = new Date(t).toISOString().slice(0, 10);
    const google = emptyMs();
    const microsoft = emptyMs();
    const microsoftConsumer = emptyMs();
    const vendors = new Map<
      string,
      { google: MicrosoftPlacementCounts; microsoft: MicrosoftPlacementCounts; microsoftConsumer: MicrosoftPlacementCounts }
    >();
    for (const r of byDay.get(day) ?? []) {
      const v = vendors.get(r.senderVendor) ?? { google: emptyMs(), microsoft: emptyMs(), microsoftConsumer: emptyMs() };
      if (r.recipientEsp === ESP_GOOGLE) {
        add(google, r);
        add(v.google, r);
      } else if (r.recipientEsp === ESP_OUTLOOK) {
        add(microsoft, r);
        add(v.microsoft, r);
      } else if (r.recipientEsp === ESP_OUTLOOK_CONSUMER) {
        add(microsoftConsumer, r);
        add(v.microsoftConsumer, r);
      }
      vendors.set(r.senderVendor, v);
    }
    days.push({
      day,
      google: googleOnly(google),
      microsoft: finish(microsoft),
      microsoftConsumer: finish(microsoftConsumer),
      byVendor: [...vendors.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([vendor, v]) => ({
          vendor,
          google: googleOnly(v.google),
          microsoft: finish(v.microsoft),
          microsoftConsumer: finish(v.microsoftConsumer),
        })),
    });
  }
  return days;
}

export const SEED_DAILY_DEFAULT_DAYS = 90;
export const SEED_DAILY_MAX_DAYS = 400;

export async function loadSeedPlacementDaily(days: number, asOf: Date = new Date()) {
  const toDay = asOf.toISOString().slice(0, 10);
  const fromDay = new Date(Date.parse(`${toDay}T00:00:00Z`) - (days - 1) * 86_400_000).toISOString().slice(0, 10);
  const result = await db.execute(sql`
    SELECT to_char(day, 'YYYY-MM-DD') AS day, recipient_esp AS "recipientEsp", sender_vendor AS "senderVendor",
           seeds, inbox, spam, missing, inbox_focused AS "inboxFocused", inbox_other AS "inboxOther"
    FROM seed_placement_daily
    WHERE day >= ${fromDay}::date AND day <= ${toDay}::date
  `);
  const rows = rowsOf<DailyRow>(result).map((r) => ({
    ...r,
    recipientEsp: Number(r.recipientEsp),
    seeds: Number(r.seeds),
    inbox: Number(r.inbox),
    spam: Number(r.spam),
    missing: Number(r.missing),
    inboxFocused: Number(r.inboxFocused),
    inboxOther: Number(r.inboxOther),
  }));
  return {
    asOf: asOf.toISOString(),
    from: fromDay,
    to: toDay,
    days: buildSeedPlacementDays(rows, fromDay, toDay),
  };
}
