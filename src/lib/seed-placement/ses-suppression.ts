/**
 * A seed receiver of OURS that SES suppressed is lifted once it accepts mail
 * again (MON-SEED-MS365-MISSING-1120).
 *
 * Prod 2026-11-19/20: the Microsoft 365 test mailbox refused mail for a day
 * (trial ended, `451 4.4.4 … no mail-enabled subscriptions`), SES gave up on one
 * seed (`554 4.4.7 Message expired`) and put the address on the ACCOUNT
 * suppression list. From then on every SES seed to it is dropped by SES itself
 * (`OnAccountSuppressionList`), whatever the mailbox does later: once the
 * tenant is renewed, the Gandi mailboxes' seeds (all through SES since
 * SES-COLD-1119) would still read MISSING there forever.
 *
 * Rule, per receiver (pure `shouldLiftSuppression`): suppressed AND the
 * receiver FOUND a seed after the suppression was written = it accepts mail
 * again (a seed through another pipe, SMTP or Postmark, landed) → delete the
 * suppression. Never lifted on hope: a dead receiver stays suppressed, so SES
 * never burns its bounce rate on it. Only our own seed receivers are touched;
 * a prospect's suppression is never read here.
 */
import { sql } from "drizzle-orm";
import { db } from "../../db";
import type { CallerInfo } from "../key-client";
import { loadSesAccountCached, sesApi, type SesAccount } from "../aws/ses-account";
import { errorMessage } from "../db-error.js";

const CALLER: CallerInfo = { method: "POST", path: "/internal/seed-placement/sync" };

export interface SuppressionEntry {
  reason: string | null;
  lastUpdateTime: Date;
}

export interface SuppressionLiftSummary {
  suppressed: number;
  lifted: number;
  failed: number;
}

/** SES `LastUpdateTime` (epoch seconds in REST JSON, or an ISO string) → Date, null when unreadable. Pure. */
export function parseSesTimestamp(value: unknown): Date | null {
  if (typeof value === "number" && Number.isFinite(value)) return new Date(value * 1000);
  if (typeof value === "string" && value.trim() !== "") {
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  return null;
}

/** Pure: lift only when the receiver found a seed AFTER SES suppressed it. */
export function shouldLiftSuppression(entry: SuppressionEntry | null, lastFoundAt: Date | null): boolean {
  if (!entry || !lastFoundAt) return false;
  return lastFoundAt.getTime() > entry.lastUpdateTime.getTime();
}

function suppressionPath(email: string): string {
  return `/v2/email/suppression/addresses/${encodeURIComponent(email)}`;
}

/** The account-level suppression of one address, null when SES holds none (404). Throws on anything else. */
export async function readSuppression(account: SesAccount, email: string): Promise<SuppressionEntry | null> {
  const got = await sesApi(account, "GET", suppressionPath(email));
  if (got.status === 404) return null;
  if (got.status !== 200) throw new Error(`SES GET suppression ${email}: ${got.status} ${got.text.slice(0, 200)}`);
  const dest = (got.json.SuppressedDestination ?? {}) as Record<string, unknown>;
  const lastUpdateTime = parseSesTimestamp(dest.LastUpdateTime);
  if (!lastUpdateTime) throw new Error(`SES suppression ${email}: unreadable LastUpdateTime ${JSON.stringify(dest.LastUpdateTime)}`);
  return { reason: typeof dest.Reason === "string" ? dest.Reason : null, lastUpdateTime };
}

function rowsOf<T>(result: unknown): T[] {
  const rows = (result as { rows?: T[] }).rows;
  return Array.isArray(rows) ? rows : [];
}

/** Every receiver seeded in the last 30 days with the newest seed it found (null = none). */
async function loadReceiversLastFound(): Promise<{ receiverEmail: string; lastFoundAt: Date | null }[]> {
  const rows = rowsOf<{ receiverEmail: string; lastFoundAt: Date | string | null }>(
    await db.execute(sql`
      SELECT r.receiver_email AS "receiverEmail",
             (SELECT max(o.observed_at) FROM seed_placement_observations o WHERE o.receiver_email = r.receiver_email) AS "lastFoundAt"
      FROM (
        SELECT DISTINCT receiver_email FROM seed_placement_dispatches
        WHERE dispatched_at > now() - interval '30 days'
      ) r
    `),
  );
  // observed_at is a UTC timestamp without zone: read it as UTC.
  return rows.map((r) => ({
    receiverEmail: r.receiverEmail,
    lastFoundAt:
      r.lastFoundAt === null
        ? null
        : r.lastFoundAt instanceof Date
          ? r.lastFoundAt
          : new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(r.lastFoundAt) ? r.lastFoundAt : `${r.lastFoundAt.replace(" ", "T")}Z`),
  }));
}

/** Lift SES suppressions of our receivers that accept mail again. Fail-soft per receiver (logged, counted). */
export async function liftRecoveredReceiverSuppressions(): Promise<SuppressionLiftSummary> {
  const summary: SuppressionLiftSummary = { suppressed: 0, lifted: 0, failed: 0 };
  const receivers = await loadReceiversLastFound();
  if (receivers.length === 0) return summary;
  const account = await loadSesAccountCached(CALLER);
  for (const r of receivers) {
    try {
      const entry = await readSuppression(account, r.receiverEmail);
      if (!entry) continue;
      summary.suppressed += 1;
      if (!shouldLiftSuppression(entry, r.lastFoundAt)) {
        console.warn(
          `[seed-placement] receiver ${r.receiverEmail} is on the SES suppression list since ${entry.lastUpdateTime.toISOString()} (${entry.reason}); kept: no seed found there since`,
        );
        continue;
      }
      const deleted = await sesApi(account, "DELETE", suppressionPath(r.receiverEmail));
      if (deleted.status !== 200 && deleted.status !== 404) {
        throw new Error(`SES DELETE suppression: ${deleted.status} ${deleted.text.slice(0, 200)}`);
      }
      summary.lifted += 1;
      console.log(
        `[seed-placement] receiver ${r.receiverEmail} lifted from the SES suppression list (suppressed ${entry.lastUpdateTime.toISOString()}, found a seed ${r.lastFoundAt?.toISOString()})`,
      );
    } catch (error) {
      summary.failed += 1;
      console.error(`[seed-placement] SES suppression check of ${r.receiverEmail} failed: ${errorMessage(error)}`);
    }
  }
  return summary;
}
