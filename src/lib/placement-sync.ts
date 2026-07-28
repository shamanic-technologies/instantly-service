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
import { fetchTestablePoolEmails } from "./account-lifecycle-sync";
import {
  aggregatePlacementRows,
  blendEspRows,
  type LatestEspRow,
} from "./placement-promote";
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

function rowsOf<T = SilverReadRow>(result: unknown): T[] {
  if (!result) return [];
  return Array.isArray(result) ? (result as T[]) : (((result as { rows?: T[] }).rows) ?? []);
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
    // Instantly's schedule `timezone` is a fixed enum — "Etc/UTC" is rejected
    // (`must be equal to one of the allowed values`). America/Chicago is the
    // proven-accepted IANA value (createCampaign uses it); the staggered hours
    // are then Chicago-local, which is fine for a daily fleet placement test.
    timezone: "America/Chicago",
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
      delivery_mode: 1,
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

export interface RunPlacementTestSummary {
  created: number;
  testCode: string | null;
  recipientEsps: string[];
  senderCount: number;
}

/**
 * Create ONE one-time (type 1) fleet inbox-placement test that runs immediately.
 *
 * This is the plan-compatible path: automated (type 2) tests are gated to
 * Instantly HyperGrowth plans (402 on Growth), but one-time tests run on the
 * Growth Inbox Placement sub. The recurrence comes from OUR cron
 * (.github/workflows/placement-cron.yml) calling this every 6h — Instantly runs
 * each test once, server-side, from the whole workspace fleet; `sync` then pulls
 * the per-(sender, ESP) results into silver.
 *
 * Spends Growth-sub quota — caller MUST gate on `isPlacementSchedulingEnabled()`.
 * Fail loud on a create rejection (402 quota / 400).
 */
export async function runOneTimeFleetPlacementTest(
  apiKey: string,
): Promise<RunPlacementTestSummary> {
  // Instantly requires explicit sender accounts (`emails`) — a placement test
  // does NOT auto-send from the whole workspace (empty `emails` 400s: "Either
  // tags or emails must be provided"). Test the lifecycle TESTABLE pool
  // (lifecycle_status IN in_recovery | in_production — active + not brand-blocked),
  // read from silver: an in_recovery account MUST be testable to earn the
  // delivery == 100 that promotes it to in_production (seeding from in_production
  // only would deadlock — a recovering account would never get tested). Empty
  // pool → nothing to test (created 0, no Instantly call, no fabricated result).
  //
  // CADENCE: a placement test sends ~30-50 seeds/account, and a mailbox's safe
  // daily volume is ~50 (40 send + 10 warmup), so a test can NOT run on top of a
  // normal sending day. We therefore test the WHOLE testable pool ONCE PER WEEK,
  // on SATURDAY (the cron fires `0 6 * * 6`) — a send-free day: the campaign send
  // window is Mon-Fri (instantly-client.ts, Sat+Sun off) and warmup is
  // weekday-only, so on Saturday the mailbox is otherwise empty and absorbs the
  // seed spike safely; if the spike overflows the per-account daily cap, it
  // finishes on Sunday, also send-free. No per-account rotation / cursor state —
  // every testable account is seeded the same Saturday.
  const senders = await fetchTestablePoolEmails();
  if (senders.length === 0) {
    return { created: 0, testCode: null, recipientEsps: [], senderCount: 0 };
  }

  const espOptions = await getEmailServiceProviderOptions(apiKey);
  // Test Gmail + Outlook (the two ESPs the deliverability finding hinges on).
  const recipientsLabels = espOptions.filter(
    (o) => o.esp === "Google" || o.esp === "Outlook",
  );

  const testCode = `${PLACEMENT_TEST_CODE_PREFIX}_onetime`;
  await createInboxPlacementTest(apiKey, {
    name: "Fleet inbox placement (one-time)",
    type: 1,
    sending_method: 1,
    delivery_mode: 1,
    email_subject: "Quick question",
    email_body: "Hi, just checking in on the note I sent over. Any thoughts?",
    emails: senders,
    recipients_labels: recipientsLabels,
    text_only: true,
    test_code: testCode,
    run_immediately: true,
  });

  return {
    created: 1,
    testCode,
    recipientEsps: recipientsLabels.map((o) => o.esp),
    senderCount: senders.length,
  };
}

/** test_code marker on the immediate "this account has never been tested" tests. */
export const UNTESTED_PLACEMENT_TEST_CODE = `${PLACEMENT_TEST_CODE_PREFIX}_untested`;

/**
 * How long a just-created test suppresses a re-test of the accounts it seeded.
 * A test takes hours to finish and its results only reach silver on the next
 * `sync`, so without this window the hourly cron would re-create a test for the
 * same never-tested account every hour until the results land.
 */
export const UNTESTED_RETEST_SUPPRESSION_HOURS = 48;

/**
 * Testable accounts (lifecycle in_recovery | in_production) that have NEVER been
 * placement-tested — i.e. zero silver rows — and were not already seeded into a
 * test we created in the last {@link UNTESTED_RETEST_SUPPRESSION_HOURS} hours.
 *
 * The suppression reads BRONZE (`instantly_placement_tests_raw`), which
 * `runUntestedPlacementTest` writes at CREATE time (not only at sync time) — that
 * write is what makes this predicate derivable with no extra state table.
 */
export async function fetchUntestedTestableEmails(): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT a.email AS "email"
    FROM instantly_accounts a
    WHERE a.lifecycle_status IN ('in_recovery', 'in_production')
      AND NOT EXISTS (
        SELECT 1 FROM instantly_placement_results r
        WHERE r.account_email = a.email
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_placement_tests_raw t
        WHERE COALESCE(
                (t.payload->>'timestamp_created')::timestamptz,
                t.fetched_at
              ) > now() - make_interval(hours => ${UNTESTED_RETEST_SUPPRESSION_HOURS})
          AND jsonb_exists(t.payload->'emails', a.email)
      )
    ORDER BY a.email
  `);
  return rowsOf<{ email: string }>(result)
    .map((r) => r.email)
    .filter(Boolean);
}

/**
 * Create ONE immediate one-time (type 1) placement test seeded with ONLY the
 * never-placement-tested accounts.
 *
 * Why this exists on top of the weekly Saturday full-pool test: a testable account
 * that appears mid-week (a fresh DFY/Primeforge mailbox lands in_recovery because
 * delivery is UNKNOWN) would otherwise wait for the next Saturday — and then wait
 * again for the following `sync` before its result reaches silver. That is up to
 * two weeks stuck in_recovery, sending nothing. An account with no placement data
 * at all can only escape in_recovery by being tested, so it gets its own test the
 * hour it shows up.
 *
 * Empty set → NO Instantly call, no quota spent, `created: 0` (never a fabricated
 * result). Spends Growth-sub quota when non-empty — caller MUST gate on
 * `isPlacementSchedulingEnabled()`. Fail loud on a create rejection (402/400).
 */
export async function runUntestedPlacementTest(
  apiKey: string,
): Promise<RunPlacementTestSummary> {
  const senders = await fetchUntestedTestableEmails();
  if (senders.length === 0) {
    return { created: 0, testCode: null, recipientEsps: [], senderCount: 0 };
  }

  const espOptions = await getEmailServiceProviderOptions(apiKey);
  const recipientsLabels = espOptions.filter(
    (o) => o.esp === "Google" || o.esp === "Outlook",
  );

  const test = await createInboxPlacementTest(apiKey, {
    name: "Inbox placement (never-tested accounts)",
    type: 1,
    sending_method: 1,
    delivery_mode: 1,
    email_subject: "Quick question",
    email_body: "Hi, just checking in on the note I sent over. Any thoughts?",
    emails: senders,
    recipients_labels: recipientsLabels,
    text_only: true,
    test_code: UNTESTED_PLACEMENT_TEST_CODE,
    run_immediately: true,
  });

  // Bronze at CREATE time — this is what suppresses a re-test of the same
  // accounts on the next hourly run (results take hours and only reach silver on
  // the next `sync`). `emails` is forced onto the payload because the create
  // RESPONSE does not always echo the senders back; a later `sync` overwrites
  // this row with Instantly's own listed payload (onConflictDoUpdate), so bronze
  // self-corrects to the true mirror.
  await db
    .insert(instantlyPlacementTestsRaw)
    .values({
      testId: test.id,
      testCode: test.test_code ?? UNTESTED_PLACEMENT_TEST_CODE,
      payload: { ...(test as unknown as Record<string, unknown>), emails: senders },
    })
    .onConflictDoUpdate({
      target: instantlyPlacementTestsRaw.testId,
      set: {
        payload: { ...(test as unknown as Record<string, unknown>), emails: senders },
        fetchedAt: new Date(),
      },
    });

  return {
    created: 1,
    testCode: UNTESTED_PLACEMENT_TEST_CODE,
    recipientEsps: recipientsLabels.map((o) => o.esp),
    senderCount: senders.length,
  };
}
