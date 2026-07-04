/**
 * Inbox-placement ingestion + gold reads (IO glue around placement-promote).
 *
 *   syncPlacement()               — poll Instantly tests + analytics → bronze → silver.
 *   fetchLatestPlacementByAccount — gold: latest test per account, blended across ESP.
 *   fetchPlacementHistory         — gold: per-account test-over-time series.
 *   ensurePlacementSchedule       — create/maintain the recurring automated tests
 *                                   (kill-switched — spends the Growth quota).
 *
 * See CLAUDE.md "Inbox-placement history (Bronze/Silver/Gold)".
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import {
  instantlyPlacementTestsRaw,
  instantlyPlacementAnalyticsRaw,
  instantlyPlacementResults,
} from "../db/schema";
import {
  listInboxPlacementTests,
  listInboxPlacementAnalytics,
  createInboxPlacementTest,
  getEmailServiceProviderOptions,
  type InboxPlacementTest,
} from "./instantly-client";
import { aggregatePlacementRows, blendEspRows, type LatestEspRow } from "./placement-promote";
import type { InboxPlacement } from "./account-health";

/** ptid_ marker on tests THIS service creates (distinguishes ours from manual UI tests). */
export const PLACEMENT_TEST_CODE_PREFIX = "ptid_autohealth";

/** How many automated placement tests to run per day (staggered evenly). Lower to 2 if the Growth quota can't absorb 4. */
export const PLACEMENT_TESTS_PER_DAY = 4;

/** Kill-switch for the CREATE path (spends Growth-sub quota). Read path is always safe. */
export function isPlacementSchedulingEnabled(): boolean {
  return process.env.PLACEMENT_TESTS_ENABLED === "true";
}

function testedAtOf(test: InboxPlacementTest): Date {
  const t = test.timestamp_created;
  return t ? new Date(t) : new Date();
}

export interface PlacementSyncSummary {
  testsSeen: number;
  testsPromoted: number;
  analyticsRows: number;
  silverRows: number;
}

/**
 * Poll every placement test + its analytics rows, mirror to bronze (append-only,
 * deduped), and promote to silver (per (test, account, ESP), upserted). Idempotent
 * + resumable: re-runs re-sweep live state and upsert. Fail loud — a bad fetch
 * propagates (the caller counts it), no silent zero.
 */
export async function syncPlacement(apiKey: string): Promise<PlacementSyncSummary> {
  const tests = await listInboxPlacementTests(apiKey);
  const summary: PlacementSyncSummary = {
    testsSeen: tests.length,
    testsPromoted: 0,
    analyticsRows: 0,
    silverRows: 0,
  };

  for (const test of tests) {
    await db
      .insert(instantlyPlacementTestsRaw)
      .values({
        testId: test.id,
        testCode: test.test_code ?? null,
        payload: test as unknown as Record<string, unknown>,
      })
      .onConflictDoUpdate({
        target: instantlyPlacementTestsRaw.testId,
        set: { payload: test as unknown as Record<string, unknown>, fetchedAt: new Date() },
      });

    const rows = await listInboxPlacementAnalytics(apiKey, test.id);
    if (rows.length === 0) continue;

    for (const row of rows) {
      await db
        .insert(instantlyPlacementAnalyticsRaw)
        .values({
          analyticsId: row.id,
          testId: test.id,
          payload: row as unknown as Record<string, unknown>,
        })
        .onConflictDoNothing({ target: instantlyPlacementAnalyticsRaw.analyticsId });
    }
    summary.analyticsRows += rows.length;

    const silver = aggregatePlacementRows(rows, test.id, testedAtOf(test));
    for (const s of silver) {
      await db
        .insert(instantlyPlacementResults)
        .values(s)
        .onConflictDoUpdate({
          target: [
            instantlyPlacementResults.testId,
            instantlyPlacementResults.accountEmail,
            instantlyPlacementResults.recipientEsp,
          ],
          set: {
            testedAt: s.testedAt,
            seedTotal: s.seedTotal,
            inboxCount: s.inboxCount,
            spamCount: s.spamCount,
            missingCount: s.missingCount,
            inboxPct: s.inboxPct,
            spamPct: s.spamPct,
            missingPct: s.missingPct,
            spfPass: s.spfPass,
            dkimPass: s.dkimPass,
            dmarcPass: s.dmarcPass,
          },
        });
    }
    summary.silverRows += silver.length;
    summary.testsPromoted += 1;
  }

  return summary;
}

type SilverReadRow = {
  account_email: string;
  recipient_esp: number;
  tested_at: string;
  seed_total: number;
  inbox_count: number;
  spam_count: number;
  missing_count: number;
};

function rowsOf(result: unknown): SilverReadRow[] {
  if (!result) return [];
  return Array.isArray(result)
    ? (result as SilverReadRow[])
    : (((result as { rows?: SilverReadRow[] }).rows) ?? []);
}

function toLatestEspRow(r: SilverReadRow): LatestEspRow {
  return {
    inboxCount: Number(r.inbox_count),
    spamCount: Number(r.spam_count),
    missingCount: Number(r.missing_count),
    seedTotal: Number(r.seed_total),
    testedAt: new Date(r.tested_at),
  };
}

/**
 * Gold: latest placement per account, blended across ESP. Reads only the newest
 * test per account (`tested_at = MAX per account`). Returns a Map keyed by
 * account email; an account with no placement data is simply absent (→ null in
 * the account-health mapper).
 */
export async function fetchLatestPlacementByAccount(): Promise<Map<string, InboxPlacement>> {
  const result = await db.execute(sql`
    SELECT r.account_email, r.recipient_esp, r.tested_at,
           r.seed_total, r.inbox_count, r.spam_count, r.missing_count
    FROM instantly_placement_results r
    WHERE r.tested_at = (
      SELECT MAX(r2.tested_at) FROM instantly_placement_results r2
      WHERE r2.account_email = r.account_email
    )
  `);

  const byAccount = new Map<string, LatestEspRow[]>();
  for (const row of rowsOf(result)) {
    const list = byAccount.get(row.account_email) ?? [];
    list.push(toLatestEspRow(row));
    byAccount.set(row.account_email, list);
  }

  const out = new Map<string, InboxPlacement>();
  for (const [email, rows] of byAccount) {
    const blended = blendEspRows(rows);
    if (blended) out.set(email, blended);
  }
  return out;
}

export interface PlacementHistoryEntry extends InboxPlacement {
  testId: string;
}

/**
 * Gold: per-account placement history — one blended entry per test, newest first.
 */
export async function fetchPlacementHistory(
  accountEmail: string,
): Promise<PlacementHistoryEntry[]> {
  const result = await db.execute(sql`
    SELECT r.test_id, r.recipient_esp, r.tested_at,
           r.seed_total, r.inbox_count, r.spam_count, r.missing_count
    FROM instantly_placement_results r
    WHERE r.account_email = ${accountEmail}
    ORDER BY r.tested_at DESC
  `);

  const byTest = new Map<string, { rows: LatestEspRow[]; testedAt: Date }>();
  const order: string[] = [];
  for (const row of rowsOf(result) as (SilverReadRow & { test_id: string })[]) {
    let g = byTest.get(row.test_id);
    if (!g) {
      g = { rows: [], testedAt: new Date(row.tested_at) };
      byTest.set(row.test_id, g);
      order.push(row.test_id);
    }
    g.rows.push(toLatestEspRow(row));
  }

  const out: PlacementHistoryEntry[] = [];
  for (const testId of order) {
    const g = byTest.get(testId)!;
    const blended = blendEspRows(g.rows);
    if (blended) out.push({ testId, ...blended });
  }
  return out;
}

/** Even-staggered schedule for the Nth automated test of the day (00:00, 06:00, …). */
function staggeredSchedule(index: number, perDay: number) {
  const hour = Math.floor((24 / perDay) * index);
  const hh = String(hour).padStart(2, "0");
  return {
    // Instantly wants an OBJECT keyed by day-of-week (0=Sunday..6=Saturday), NOT
    // an array (an array 400s: `body/schedule/days must be object`). Every day —
    // placement seed sends are not cold outreach, so weekends are fine.
    days: { "0": true, "1": true, "2": true, "3": true, "4": true, "5": true, "6": true },
    // A one-hour send window at the staggered hour. `to` is required alongside `from`.
    timing: { from: `${hh}:00`, to: `${hh}:59` },
    timezone: "Etc/UTC",
  };
}

export interface EnsureScheduleSummary {
  existing: number;
  created: number;
  perDay: number;
}

/**
 * Ensure `PLACEMENT_TESTS_PER_DAY` automated (type 2) placement tests exist,
 * staggered across the day, so Instantly runs the fleet placement test on a
 * recurring schedule server-side (no external cron). Idempotent: counts our
 * existing tests (by `PLACEMENT_TEST_CODE_PREFIX`) and creates only the missing
 * ones. Spends Growth-sub quota — caller MUST gate on `isPlacementSchedulingEnabled()`.
 * Fail loud on a create rejection (402 quota / 400) — no silent skip.
 */
export async function ensurePlacementSchedule(apiKey: string): Promise<EnsureScheduleSummary> {
  const perDay = PLACEMENT_TESTS_PER_DAY;
  const tests = await listInboxPlacementTests(apiKey);
  const ours = tests.filter(
    (t) => t.type === 2 && (t.test_code ?? "").startsWith(PLACEMENT_TEST_CODE_PREFIX),
  );

  const espOptions = await getEmailServiceProviderOptions(apiKey);
  // Test Gmail + Outlook (the two ESPs the deliverability finding hinges on).
  const recipientsLabels = espOptions.filter(
    (o) => o.esp === "Google" || o.esp === "Outlook",
  );

  let created = 0;
  for (let i = ours.length; i < perDay; i++) {
    await createInboxPlacementTest(apiKey, {
      name: `Fleet inbox placement #${i + 1}`,
      type: 2,
      sending_method: 1,
      email_subject: "Quick question",
      email_body: "Hi, just checking in on the note I sent over. Any thoughts?",
      emails: [],
      recipients_labels: recipientsLabels,
      text_only: true,
      test_code: `${PLACEMENT_TEST_CODE_PREFIX}_${i + 1}`,
      status: 1,
      schedule: staggeredSchedule(i, perDay),
    });
    created += 1;
  }

  return { existing: ours.length, created, perDay };
}
