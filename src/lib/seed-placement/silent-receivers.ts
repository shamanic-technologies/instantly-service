/**
 * A seed receiver that suddenly reads NOTHING is broken (MON-SEED-MS365-MISSING-1120).
 *
 * Prod 2026-11-20: the Microsoft 365 test mailbox read 0 of 205 seeds (11-11: 79 %
 * inbox, 0 missing) and nothing turned red: its token refreshed fine, the sync
 * reported no failed receiver, the tenant simply stopped ACCEPTING mail (its
 * 30-day Business Basic trial ended, Exchange Online `Disabled`; Microsoft
 * answered `451 4.4.4 … hosted tenant which has no mail-enabled subscriptions`,
 * then SES put the address on its suppression list). A token check cannot see
 * that; only the seeds can.
 *
 * Rule, per receiver (Google, Microsoft 365, Outlook.com alike): the seeds we
 * sent it AFTER the last one it found, and before the newest sync that read the
 * other receivers (minus a settle margin), are all unfound and number at least
 * {@link SILENT_RECEIVER_MIN_SEEDS}. A receiver that finds even one seed of a
 * new test moves its "last found" and goes green on its own; a few unfound
 * seeds (normal loss) never reach the floor.
 */
import { sql } from "drizzle-orm";
import { db } from "../../db";
import { ESP_GOOGLE, ESP_OUTLOOK, ESP_OUTLOOK_CONSUMER } from "./seeds";

/** Unfound seeds in a row that make a receiver silent (a weekly test sends ~200 to each). */
export const SILENT_RECEIVER_MIN_SEEDS = 20;
/** A seed younger than the newest sync minus this may simply not have arrived when it read. */
export const SILENT_RECEIVER_SETTLE_MINUTES = 30;
/** How far back unfound seeds are counted. */
export const SILENT_RECEIVER_WINDOW_DAYS = 14;

export interface ReceiverUnfoundRow {
  receiverEmail: string;
  recipientEsp: number;
  /** Seeds sent since its last finding, settled before the newest sync. */
  unfound: number;
  /** First of those seeds. */
  since: Date | string;
  /** Last seed it found, null = never. */
  lastFoundAt: Date | string | null;
  /** The newest bounce / delay notice naming it, verbatim, null = none. */
  lastRefusal: string | null;
}

export interface SilentReceiver {
  receiverEmail: string;
  provider: string;
  unfound: number;
  since: string;
  lastFoundAt: string | null;
  lastRefusal: string | null;
}

export function providerLabel(esp: number): string {
  if (esp === ESP_GOOGLE) return "Google";
  if (esp === ESP_OUTLOOK) return "Microsoft 365";
  if (esp === ESP_OUTLOOK_CONSUMER) return "Outlook.com";
  return `provider ${esp}`;
}

function iso(v: Date | string): string {
  return (v instanceof Date ? v : new Date(v)).toISOString();
}

/** The receiver's refusal reason out of a DSN text: its Diagnostic-Code line when present (pure). */
export function refusalReason(text: string | null): string | null {
  if (!text) return null;
  const diag = /Diagnostic-Code:\s*(?:smtp;\s*)?([^\n]+)/i.exec(text)?.[1];
  return (diag ?? text.split("\n")[0]).trim().slice(0, 320) || null;
}

/** Pure: the receivers that count as silent. */
export function judgeSilentReceivers(rows: readonly ReceiverUnfoundRow[], minSeeds = SILENT_RECEIVER_MIN_SEEDS): SilentReceiver[] {
  return rows
    .filter((r) => r.unfound >= minSeeds)
    .map((r) => ({
      receiverEmail: r.receiverEmail,
      provider: providerLabel(r.recipientEsp),
      unfound: r.unfound,
      since: iso(r.since),
      lastFoundAt: r.lastFoundAt === null ? null : iso(r.lastFoundAt),
      lastRefusal: refusalReason(r.lastRefusal),
    }))
    .sort((a, b) => (a.receiverEmail < b.receiverEmail ? -1 : 1));
}

/** One plain sentence per silent receiver: what the Monitoring hub prints. */
export function silentReceiverProblem(r: SilentReceiver): string {
  return (
    `${r.provider} test mailbox ${r.receiverEmail} received none of the ${r.unfound} test emails sent to it since ${r.since.slice(0, 16).replace("T", " ")} UTC` +
    (r.lastFoundAt ? ` (last one found ${r.lastFoundAt.slice(0, 10)})` : " (never found one)") +
    (r.lastRefusal ? `; its server answers: ${r.lastRefusal}` : "")
  );
}

function rowsOf<T>(result: unknown): T[] {
  const rows = (result as { rows?: T[] }).rows;
  return Array.isArray(rows) ? rows : [];
}

/** Read every receiver's unfound run, then the newest refusal naming each one that crosses the floor. */
export async function loadSilentReceivers(asOf: Date = new Date()): Promise<SilentReceiver[]> {
  const windowStart = new Date(asOf.getTime() - SILENT_RECEIVER_WINDOW_DAYS * 86_400_000);
  const rows = rowsOf<Omit<ReceiverUnfoundRow, "lastRefusal">>(
    await db.execute(sql`
      WITH newest_read AS (
        SELECT max(observed_at) AS at FROM seed_placement_observations
      ), last_found AS (
        SELECT receiver_email, max(observed_at) AS at
        FROM seed_placement_observations
        GROUP BY receiver_email
      )
      SELECT d.receiver_email AS "receiverEmail",
             min(d.recipient_esp)::int AS "recipientEsp",
             count(*)::int AS "unfound",
             min(d.dispatched_at) AS "since",
             max(lf.at) AS "lastFoundAt"
      FROM seed_placement_dispatches d
      CROSS JOIN newest_read nr
      LEFT JOIN last_found lf ON lf.receiver_email = d.receiver_email
      WHERE d.outcome = 'sent'
        AND d.dispatched_at > ${windowStart.toISOString()}::timestamp
        AND d.dispatched_at < nr.at - make_interval(mins => ${SILENT_RECEIVER_SETTLE_MINUTES})
        AND (lf.at IS NULL OR d.dispatched_at > lf.at)
        -- A Microsoft receiver we retired (or that is not authorized) is no receiver any more.
        AND NOT EXISTS (
          SELECT 1 FROM microsoft_seed_mailboxes m
          WHERE m.email = d.receiver_email AND m.status <> 'authorized'
        )
        AND NOT EXISTS (
          SELECT 1 FROM seed_placement_observations o
          WHERE o.receiver_email = d.receiver_email AND o.message_id = d.message_id
        )
      GROUP BY d.receiver_email
      HAVING count(*) >= ${SILENT_RECEIVER_MIN_SEEDS}
    `),
  );
  const out: ReceiverUnfoundRow[] = [];
  for (const r of rows) {
    const refusal = rowsOf<{ text: string | null }>(
      await db.execute(sql`
        SELECT COALESCE(payload->>'textSnippet', subject) AS text
        FROM imap_messages_raw
        WHERE kind IN ('bounce', 'delay')
          AND polled_at > ${iso(r.since)}::timestamp
          AND payload::text ILIKE ${`%${r.receiverEmail}%`}
          AND COALESCE(payload->'sesEvent'->>'bounceSubType', '') <> 'OnAccountSuppressionList'
        ORDER BY polled_at DESC
        LIMIT 1
      `),
    )[0];
    out.push({ ...r, lastRefusal: refusal?.text ?? null });
  }
  return judgeSilentReceivers(out);
}
