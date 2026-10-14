/**
 * Delivery split by what our verifier said about the address AT SEND TIME.
 *
 * Fleet-wide, every org, since the first email we ever sent. Served to
 * features-service (public channel catalogue), which quotes it in client copy:
 * "how many of the emails we send are delivered, by the verdict the address had".
 *
 * BASIS: one row of `instantly_lead_status_current` with `sent` = one SEQUENCE
 * (one address emailed under one campaign) whose first real email left. That is
 * the same population and the same `sent` / `delivered` / `bounced` flags every
 * gold stats read serves (delivered = sent AND NOT bounced, bounced = any step of
 * the sequence bounced), so the buckets reconcile with our own total.
 *
 * VERDICT AT SEND TIME: the latest verdict our verifier (BounceVerify, through
 * apollo-service) returned for the address at or before the sequence's FIRST real
 * email. Read from our mirror of apollo-service's verification history
 * (`email_verification_history`, lib/verification-history-mirror.ts), every org,
 * every source (serve-time verification, the send door, email finders).
 *  - a verdict on record before the send: that verdict (latest decisive one wins
 *    over a later call that returned nothing);
 *  - only calls that returned NO verdict before the send: `no_verdict`;
 *  - no verification at all before the send: `never_verified`. Never invented.
 *
 * Sends after the mirror's last complete sync are LEFT OUT (`sentThrough`): a
 * verification apollo made just before such a send may not be mirrored yet, and
 * counting it as `never_verified` would invent a fact. The population and the
 * reconciling total both stop at the same instant.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";

/** Every bucket, in the order served. Closed: apollo-service's verdict set + the two non-verdicts. */
export const VERDICT_BUCKETS = [
  "valid",
  "catch_all",
  "unknown",
  "invalid",
  "risky",
  "no_verdict",
  "never_verified",
] as const;

export type VerdictBucket = (typeof VERDICT_BUCKETS)[number];

export interface DeliveryFigures {
  sent: number;
  delivered: number;
  bounced: number;
  /** delivered / sent, 4 decimals; null when nothing was sent (no rate over zero). */
  deliveryRate: number | null;
  /** bounced / sent, 4 decimals; null when nothing was sent. */
  bounceRate: number | null;
}

export interface DeliveryByVerdict {
  basis: "sequences";
  /** First real email of the earliest sequence counted. Null when nothing was sent. */
  since: string | null;
  /** Sequences whose first email left after this instant are not counted yet. */
  sentThrough: string;
  /** Earliest verification on record (apollo-service started verifying then). */
  verificationsSince: string | null;
  buckets: Array<{ verdict: VerdictBucket } & DeliveryFigures>;
  /**
   * Same sequences, bucketed by the LATEST verdict on record today, including
   * verifications made AFTER the send. Answers "how do addresses our verifier
   * calls X fare", not "what did we know when sending". Present only on the
   * full read (loadDeliveryByVerdict).
   */
  bucketsByLatestVerdict?: Array<{ verdict: VerdictBucket } & DeliveryFigures>;
  /** Sum of the buckets, checked against an independent count of the same population. */
  total: DeliveryFigures;
  /** Sequences whose first email left after `sentThrough`: counted on the next sync. */
  sentAfterThrough: number;
}

export interface BucketRow {
  bucket: string;
  sent: number;
  bounced: number;
}

export interface TotalRow {
  sent: number;
  bounced: number;
  since: Date | string | null;
  /** Every sent sequence, no cutoff. */
  sent_all: number;
}

function rate(part: number, whole: number): number | null {
  if (whole === 0) return null;
  return Math.round((part / whole) * 10_000) / 10_000;
}

function figures(sent: number, bounced: number): DeliveryFigures {
  const delivered = sent - bounced;
  return { sent, delivered, bounced, deliveryRate: rate(delivered, sent), bounceRate: rate(bounced, sent) };
}

function iso(value: Date | string | null): string | null {
  if (value === null) return null;
  const d = value instanceof Date ? value : new Date(value);
  return d.toISOString();
}

/**
 * Pure: assemble the response from the grouped rows and the independent total.
 * Every bucket is present (zero when absent). FAILS LOUD on a verdict outside the
 * closed set (a new apollo verdict must be named here, never folded silently) and
 * when the buckets do not add up to the total (the partition is the promise).
 */
export function assembleDeliveryByVerdict(
  rows: BucketRow[],
  total: TotalRow,
  sentThrough: Date,
  verificationsSince: Date | string | null,
): DeliveryByVerdict {
  const byBucket = new Map<string, BucketRow>();
  for (const row of rows) {
    if (!(VERDICT_BUCKETS as readonly string[]).includes(row.bucket)) {
      throw new Error(
        `delivery-by-verdict: verdict "${row.bucket}" (${row.sent} sequences) is not a known bucket; name it in VERDICT_BUCKETS`,
      );
    }
    byBucket.set(row.bucket, row);
  }

  const buckets = VERDICT_BUCKETS.map((verdict) => {
    const row = byBucket.get(verdict);
    return { verdict, ...figures(Number(row?.sent ?? 0), Number(row?.bounced ?? 0)) };
  });

  const sumSent = buckets.reduce((s, b) => s + b.sent, 0);
  const sumBounced = buckets.reduce((s, b) => s + b.bounced, 0);
  if (sumSent !== Number(total.sent) || sumBounced !== Number(total.bounced)) {
    throw new Error(
      `delivery-by-verdict: buckets do not reconcile (sent ${sumSent} vs ${total.sent}, bounced ${sumBounced} vs ${total.bounced})`,
    );
  }

  return {
    basis: "sequences",
    since: iso(total.since),
    sentThrough: sentThrough.toISOString(),
    verificationsSince: iso(verificationsSince),
    buckets,
    total: figures(Number(total.sent), Number(total.bounced)),
    sentAfterThrough: Number(total.sent_all) - Number(total.sent),
  };
}

function rowsOf<T>(result: unknown): T[] {
  return ((result as { rows?: T[] }).rows ?? []) as T[];
}

/**
 * Grouped read. `cutAtSend` true = only verifications at or before the
 * sequence's first email (what we knew when sending); false = the latest
 * verification on record today, including ones made after the send.
 */
function groupedQuery(cutoff: string, cutAtSend: boolean) {
  const atSend = cutAtSend ? sql`AND v.verified_at <= seq.first_sent_at` : sql``;
  return sql`
      WITH seq AS (
        SELECT lower(trim(s.lead_email)) AS email,
               (s.first_sent_at AT TIME ZONE 'UTC') AS first_sent_at,
               s.bounced
        FROM instantly_lead_status_current s
        WHERE s.sent
          AND s.first_sent_at IS NOT NULL
          AND (s.first_sent_at AT TIME ZONE 'UTC') <= ${cutoff}::timestamptz
      )
      SELECT
        CASE
          WHEN d.verdict IS NOT NULL THEN d.verdict
          WHEN n.any_call THEN 'no_verdict'
          ELSE 'never_verified'
        END AS bucket,
        count(*)::int AS sent,
        count(*) FILTER (WHERE seq.bounced)::int AS bounced
      FROM seq
      LEFT JOIN LATERAL (
        SELECT v.verdict
        FROM email_verification_history v
        WHERE v.email = seq.email
          AND v.verdict IS NOT NULL
          ${atSend}
        ORDER BY v.verified_at DESC
        LIMIT 1
      ) d ON TRUE
      LEFT JOIN LATERAL (
        SELECT TRUE AS any_call
        FROM email_verification_history v
        WHERE v.email = seq.email
          ${atSend}
        LIMIT 1
      ) n ON TRUE
      GROUP BY 1
  `;
}

/**
 * Read the split. `sentThrough` = the mirror's last complete sync (caller passes
 * it). The grouped reads (at send, latest on record) and an independent total
 * over the same population, so the reconciliation check is real, not a sum of itself.
 */
export async function loadDeliveryByVerdict(sentThrough: Date): Promise<DeliveryByVerdict> {
  const cutoff = sentThrough.toISOString();

  const [grouped, groupedLatest, totals, firstVerification] = await Promise.all([
    db.execute(groupedQuery(cutoff, true)),
    db.execute(groupedQuery(cutoff, false)),
    db.execute(sql`
      SELECT count(*) FILTER (WHERE s.counted)::int AS sent,
             count(*) FILTER (WHERE s.counted AND s.bounced)::int AS bounced,
             min(s.first_sent_at) FILTER (WHERE s.counted) AS since,
             count(*)::int AS sent_all
      FROM (
        SELECT (x.first_sent_at AT TIME ZONE 'UTC') AS first_sent_at,
               x.bounced,
               (x.first_sent_at IS NOT NULL
                 AND (x.first_sent_at AT TIME ZONE 'UTC') <= ${cutoff}::timestamptz) AS counted
        FROM instantly_lead_status_current x
        WHERE x.sent
      ) s
    `),
    db.execute(sql`SELECT min(verified_at) AS since FROM email_verification_history`),
  ]);

  const total = rowsOf<TotalRow>(totals)[0] ?? { sent: 0, bounced: 0, since: null, sent_all: 0 };
  const verificationsSince = rowsOf<{ since: Date | string | null }>(firstVerification)[0]?.since ?? null;
  const atSend = assembleDeliveryByVerdict(rowsOf<BucketRow>(grouped), total, sentThrough, verificationsSince);
  const latest = assembleDeliveryByVerdict(rowsOf<BucketRow>(groupedLatest), total, sentThrough, verificationsSince);
  return { ...atSend, bucketsByLatestVerdict: latest.buckets };
}
