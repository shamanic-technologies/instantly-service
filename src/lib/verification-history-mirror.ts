/**
 * Our copy of apollo-service's verification history: every BounceVerify call it
 * ever made, every org, every source, with its verdict (or none). Read by
 * lib/delivery-by-verdict.ts to say which verdict an address had when we first
 * emailed it. apollo-service owns the history; we read it through its feed (no
 * cross-service DB read) and keep a cursor.
 *
 * One pass pages from the stored cursor to the end of the feed, upserting each
 * row (idempotent on the verification id) and saving the cursor after every
 * page, so a pass that dies resumes where it stopped. Once a pass reaches the
 * end, `caught_up_at` = the instant the pass STARTED minus the feed's settle
 * window and a margin (`CAUGHT_UP_LAG_MS`): apollo holds back rows younger than
 * 60 s, so only verifications older than that are guaranteed in the copy. That
 * instant is delivery-by-verdict's `sentThrough` cutoff. FAILS LOUD: a page that cannot be read or
 * written throws; the cursor and `caught_up_at` stay where they were.
 *
 * Driven by its own worker (first pass 60 s after boot, then every 5 minutes)
 * and by POST /internal/delivery-by-verdict/sync for ops.
 */

import { eq, sql } from "drizzle-orm";

import { db } from "../db";
import { emailVerificationHistory, emailVerificationHistorySync } from "../db/schema";
import { fetchVerificationHistoryPage, type VerificationHistoryRow } from "./apollo-client";
import { normalizeLeadEmail } from "./lead-email";

export const VERIFICATION_FEED = "apollo-email-verifications";

/** Pages read per pass, bounded so a pass never runs unbounded; the next pass continues. */
export const MAX_PAGES_PER_PASS = 200;

/** apollo's settle window (60 s) + margin: the copy is complete up to pass start minus this. */
export const CAUGHT_UP_LAG_MS = 2 * 60 * 1000;

export interface MirrorPassResult {
  pages: number;
  rows: number;
  caughtUp: boolean;
  cursor: string | null;
}

async function readState(): Promise<{ cursor: string | null; caughtUpAt: Date | null } | null> {
  const rows = await db
    .select({ cursor: emailVerificationHistorySync.cursor, caughtUpAt: emailVerificationHistorySync.caughtUpAt })
    .from(emailVerificationHistorySync)
    .where(eq(emailVerificationHistorySync.feed, VERIFICATION_FEED))
    .limit(1);
  return rows[0] ?? null;
}

/** When the copy last reached the end of the feed; null = never. */
export async function readMirrorCaughtUpAt(): Promise<Date | null> {
  return (await readState())?.caughtUpAt ?? null;
}

async function writeState(cursor: string | null, caughtUpAt?: Date): Promise<void> {
  const set: Record<string, unknown> = { cursor, updatedAt: new Date() };
  if (caughtUpAt) set.caughtUpAt = caughtUpAt;
  await db
    .insert(emailVerificationHistorySync)
    .values({ feed: VERIFICATION_FEED, cursor, caughtUpAt: caughtUpAt ?? null })
    .onConflictDoUpdate({ target: emailVerificationHistorySync.feed, set });
}

async function upsertRows(rows: VerificationHistoryRow[]): Promise<void> {
  if (rows.length === 0) return;
  await db
    .insert(emailVerificationHistory)
    .values(
      rows.map((r) => ({
        verificationId: r.id,
        email: normalizeLeadEmail(r.email),
        verdict: r.verdict,
        verifiedAt: new Date(r.verifiedAt),
      })),
    )
    .onConflictDoUpdate({
      target: emailVerificationHistory.verificationId,
      set: {
        email: sql`excluded.email`,
        verdict: sql`excluded.verdict`,
        verifiedAt: sql`excluded.verified_at`,
      },
    });
}

let inFlight: Promise<MirrorPassResult> | null = null;

/** One pass. Concurrent callers join the pass already running. */
export function syncVerificationHistory(): Promise<MirrorPassResult> {
  if (inFlight) return inFlight;
  inFlight = runPass().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

async function runPass(): Promise<MirrorPassResult> {
  const startedAt = new Date();
  let cursor = (await readState())?.cursor ?? null;
  let pages = 0;
  let rows = 0;

  while (pages < MAX_PAGES_PER_PASS) {
    const page = await fetchVerificationHistoryPage(cursor);
    pages += 1;
    rows += page.rows.length;
    await upsertRows(page.rows);
    cursor = page.nextCursor ?? cursor;
    if (!page.hasMore) {
      await writeState(cursor, new Date(startedAt.getTime() - CAUGHT_UP_LAG_MS));
      return { pages, rows, caughtUp: true, cursor };
    }
    await writeState(cursor);
  }
  return { pages, rows, caughtUp: false, cursor };
}
