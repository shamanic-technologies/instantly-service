/**
 * Placement tests as units (MAILBOX-MONITORING-1019): the list of tests a
 * sender, mailbox, domain or the fleet took part in, and ONE test seed by seed.
 *
 * Two kinds of test share the silver `instantly_placement_results`:
 *   - `seed:<uuid>` our own seed harness: every seed is a bronze row
 *     (`seed_placement_dispatches` = sent, `seed_placement_observations` = where
 *     it landed), so the detail lists every seed, the never-found ones included
 *     (`missing`).
 *   - any other id: an Instantly paid test (cancelled 2026-10-12). Instantly only
 *     reported the seeds it FOUND (`instantly_placement_analytics_raw`), so its
 *     detail lists inbox/spam seeds and its missing ones exist only as a count.
 *
 * The list's counts are the silver rows (the numbers the lifecycle gate read);
 * the detail's counts are recomputed from the seeds it lists. Inbox providers:
 * Google (ESP 1), Microsoft 365 (ESP 2), Outlook.com (ESP 4), see
 * `seed-placement/seeds.ts`.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { ESP_GOOGLE, ESP_OUTLOOK, ESP_OUTLOOK_CONSUMER } from "../seed-placement/seeds";
import { domainOf, entityScopeSql, inScope, mailboxOf, type EntityScope } from "./entity-scope";

export type InboxProvider = "google" | "microsoft" | "microsoftConsumer" | "other";

export const INBOX_PROVIDER_LABELS: Record<InboxProvider, string> = {
  google: "Google",
  microsoft: "Microsoft 365",
  microsoftConsumer: "Outlook.com",
  other: "Other",
};

export function providerOfEsp(esp: number): InboxProvider {
  if (esp === ESP_GOOGLE) return "google";
  if (esp === ESP_OUTLOOK) return "microsoft";
  if (esp === ESP_OUTLOOK_CONSUMER) return "microsoftConsumer";
  return "other";
}

export interface ProviderCounts {
  seeds: number;
  inbox: number;
  spam: number;
  missing: number;
  /** inbox / seeds, 0..100 one decimal; null with no seed. */
  inboxPct: number | null;
}

const pct = (n: number, d: number): number | null => (d > 0 ? Math.round((n * 1000) / d) / 10 : null);

function emptyCounts(): ProviderCounts {
  return { seeds: 0, inbox: 0, spam: 0, missing: 0, inboxPct: null };
}

function addCounts(c: ProviderCounts, add: { seeds: number; inbox: number; spam: number; missing: number }): void {
  c.seeds += add.seeds;
  c.inbox += add.inbox;
  c.spam += add.spam;
  c.missing += add.missing;
  c.inboxPct = pct(c.inbox, c.seeds);
}

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

function iso(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const d = v instanceof Date ? v : new Date(String(v));
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

export function testSource(testId: string): "seed" | "instantly" {
  return testId.startsWith("seed:") ? "seed" : "instantly";
}

// ─── List ────────────────────────────────────────────────────────────────────

export interface PlacementTestSummary {
  testId: string;
  source: "seed" | "instantly";
  testedAt: string;
  senders: number;
  total: ProviderCounts;
  byProvider: Record<InboxProvider, ProviderCounts>;
}

interface SilverRow {
  test_id: string;
  account_email: string;
  recipient_esp: number | string;
  tested_at: unknown;
  seed_total: number | string;
  inbox_count: number | string;
  spam_count: number | string;
  missing_count: number | string;
}

/** Pure: one summary per test from silver rows (already scope-filtered), newest first. */
export function summarizePlacementTests(rows: readonly SilverRow[]): PlacementTestSummary[] {
  const byTest = new Map<string, { testedAt: string; senders: Set<string>; total: ProviderCounts; byProvider: Record<InboxProvider, ProviderCounts> }>();
  for (const r of rows) {
    const testedAt = iso(r.tested_at) ?? "";
    const t =
      byTest.get(r.test_id) ??
      {
        testedAt,
        senders: new Set<string>(),
        total: emptyCounts(),
        byProvider: { google: emptyCounts(), microsoft: emptyCounts(), microsoftConsumer: emptyCounts(), other: emptyCounts() },
      };
    if (testedAt > t.testedAt) t.testedAt = testedAt;
    t.senders.add(r.account_email.toLowerCase());
    const add = {
      seeds: Number(r.seed_total),
      inbox: Number(r.inbox_count),
      spam: Number(r.spam_count),
      missing: Number(r.missing_count),
    };
    addCounts(t.total, add);
    addCounts(t.byProvider[providerOfEsp(Number(r.recipient_esp))], add);
    byTest.set(r.test_id, t);
  }
  return [...byTest.entries()]
    .map(([testId, t]) => ({
      testId,
      source: testSource(testId),
      testedAt: t.testedAt,
      senders: t.senders.size,
      total: t.total,
      byProvider: t.byProvider,
    }))
    .sort((a, b) => (a.testedAt === b.testedAt ? b.testId.localeCompare(a.testId) : b.testedAt.localeCompare(a.testedAt)));
}

export function encodeTestCursor(testedAt: string, testId: string): string {
  return Buffer.from(JSON.stringify([testedAt, testId])).toString("base64url");
}

export function decodeTestCursor(cursor: string): { testedAt: string; testId: string } | null {
  try {
    const v = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
    if (Array.isArray(v) && typeof v[0] === "string" && typeof v[1] === "string") return { testedAt: v[0], testId: v[1] };
  } catch {
    // An unreadable cursor is the caller's 400, decided by the route.
  }
  return null;
}

export async function readPlacementTests(opts: {
  scope: EntityScope;
  limit: number;
  cursor: { testedAt: string; testId: string } | null;
}): Promise<{ tests: PlacementTestSummary[]; nextCursor: string | null }> {
  const scoped = entityScopeSql(opts.scope, sql`r.account_email`, sql`a.mailbox_login`);
  // Page on tests, not rows: pick the page's test ids first.
  const page = rowsOf<{ test_id: string; tested_at: unknown }>(
    await db.execute(sql`
      SELECT r.test_id, max(r.tested_at) AS tested_at
      FROM instantly_placement_results r
      LEFT JOIN instantly_accounts a ON lower(a.email) = lower(r.account_email)
      WHERE ${scoped}
      GROUP BY r.test_id
      HAVING ${
        opts.cursor
          ? sql`(max(r.tested_at), r.test_id) < (${opts.cursor.testedAt}::timestamptz AT TIME ZONE 'UTC', ${opts.cursor.testId})`
          : sql`true`
      }
      ORDER BY max(r.tested_at) DESC, r.test_id DESC
      LIMIT ${opts.limit + 1}
    `),
  );
  const ids = page.slice(0, opts.limit).map((p) => p.test_id);
  if (ids.length === 0) return { tests: [], nextCursor: null };
  const rows = rowsOf<SilverRow>(
    await db.execute(sql`
      SELECT r.test_id, r.account_email, r.recipient_esp, r.tested_at, r.seed_total, r.inbox_count, r.spam_count, r.missing_count
      FROM instantly_placement_results r
      LEFT JOIN instantly_accounts a ON lower(a.email) = lower(r.account_email)
      WHERE r.test_id = ANY(${sql.param(ids)}::text[]) AND ${scoped}
    `),
  );
  const tests = summarizePlacementTests(rows);
  const last = tests[tests.length - 1];
  return {
    tests,
    nextCursor: page.length > opts.limit && last ? encodeTestCursor(last.testedAt, last.testId) : null,
  };
}

// ─── Detail ──────────────────────────────────────────────────────────────────

export interface PlacementSeed {
  sender: string;
  senderMailbox: string;
  senderDomain: string;
  receiver: string | null;
  provider: InboxProvider;
  providerLabel: string;
  /** inbox | spam | missing; null when the seed never left (`dispatchOutcome` not `sent`). */
  placement: "inbox" | "spam" | "missing" | null;
  folder: string | null;
  /** Microsoft only: focused | other. */
  msInference: string | null;
  /** Microsoft spam confidence level. */
  msScl: number | null;
  dispatchOutcome: string;
  dispatchedAt: string | null;
  observedAt: string | null;
  spfPass: boolean | null;
  dkimPass: boolean | null;
  dmarcPass: boolean | null;
}

export interface PlacementTestDetail {
  testId: string;
  source: "seed" | "instantly";
  testedAt: string | null;
  scope: EntityScope;
  /** False for an Instantly test: its missing seeds exist only as counts. */
  missingSeedsListed: boolean;
  total: ProviderCounts;
  byProvider: Record<InboxProvider, ProviderCounts>;
  bySender: { sender: string; senderMailbox: string; total: ProviderCounts; byProvider: Record<InboxProvider, ProviderCounts> }[];
  seeds: PlacementSeed[];
}

function bool(v: unknown): boolean | null {
  return v === null || v === undefined ? null : v === true || v === "t" || v === "true";
}

/** Pure: counts from listed seeds (a seed that never left is not counted). */
export function countSeeds(seeds: readonly PlacementSeed[]): {
  total: ProviderCounts;
  byProvider: Record<InboxProvider, ProviderCounts>;
  bySender: PlacementTestDetail["bySender"];
} {
  const total = emptyCounts();
  const byProvider = { google: emptyCounts(), microsoft: emptyCounts(), microsoftConsumer: emptyCounts(), other: emptyCounts() };
  const senders = new Map<string, PlacementTestDetail["bySender"][number]>();
  for (const s of seeds) {
    if (s.placement === null) continue;
    const add = { seeds: 1, inbox: s.placement === "inbox" ? 1 : 0, spam: s.placement === "spam" ? 1 : 0, missing: s.placement === "missing" ? 1 : 0 };
    addCounts(total, add);
    addCounts(byProvider[s.provider], add);
    const row =
      senders.get(s.sender) ??
      {
        sender: s.sender,
        senderMailbox: s.senderMailbox,
        total: emptyCounts(),
        byProvider: { google: emptyCounts(), microsoft: emptyCounts(), microsoftConsumer: emptyCounts(), other: emptyCounts() },
      };
    addCounts(row.total, add);
    addCounts(row.byProvider[s.provider], add);
    senders.set(s.sender, row);
  }
  return { total, byProvider, bySender: [...senders.values()].sort((a, b) => a.sender.localeCompare(b.sender)) };
}

async function loadLogins(): Promise<Map<string, string | null>> {
  const rows = rowsOf<{ email: string; mailbox_login: string | null }>(
    await db.execute(sql`SELECT lower(email) AS email, mailbox_login FROM instantly_accounts`),
  );
  return new Map(rows.map((r) => [r.email, r.mailbox_login]));
}

export async function readPlacementTest(testId: string, scope: EntityScope): Promise<PlacementTestDetail | null> {
  const logins = await loadLogins();
  const seedOf = (sender: string, extra: Omit<PlacementSeed, "sender" | "senderMailbox" | "senderDomain" | "providerLabel">): PlacementSeed => {
    const s = sender.toLowerCase();
    return {
      sender: s,
      senderMailbox: mailboxOf(s, logins.get(s)),
      senderDomain: domainOf(s),
      providerLabel: INBOX_PROVIDER_LABELS[extra.provider],
      ...extra,
    };
  };

  let seeds: PlacementSeed[];
  if (testSource(testId) === "seed") {
    const rows = rowsOf<Record<string, unknown>>(
      await db.execute(sql`
        SELECT d.sender_email, d.receiver_email, d.recipient_esp, d.outcome, d.dispatched_at,
               o.placement, o.folder, o.ms_inference, o.ms_scl, o.observed_at, o.spf_pass, o.dkim_pass, o.dmarc_pass
        FROM seed_placement_dispatches d
        LEFT JOIN seed_placement_observations o
          ON o.message_id = d.message_id AND o.receiver_email = d.receiver_email
        WHERE d.test_id = ${testId}
        ORDER BY d.dispatched_at, d.sender_email, d.receiver_email
      `),
    );
    if (rows.length === 0) return null;
    seeds = rows.map((r) =>
      seedOf(String(r.sender_email), {
        receiver: String(r.receiver_email),
        provider: providerOfEsp(Number(r.recipient_esp)),
        placement:
          String(r.outcome) !== "sent"
            ? null
            : r.placement === "inbox" || r.placement === "spam"
              ? (r.placement as "inbox" | "spam")
              : "missing",
        folder: r.folder === null || r.folder === undefined ? null : String(r.folder),
        msInference: r.ms_inference === null || r.ms_inference === undefined ? null : String(r.ms_inference),
        msScl: r.ms_scl === null || r.ms_scl === undefined ? null : Number(r.ms_scl),
        dispatchOutcome: String(r.outcome),
        dispatchedAt: iso(r.dispatched_at),
        observedAt: iso(r.observed_at),
        spfPass: bool(r.spf_pass),
        dkimPass: bool(r.dkim_pass),
        dmarcPass: bool(r.dmarc_pass),
      }),
    );
  } else {
    const rows = rowsOf<Record<string, unknown>>(
      await db.execute(sql`
        SELECT DISTINCT ON (analytics_id)
               payload->>'sender_email' AS sender_email, payload->>'recipient_email' AS receiver_email,
               (payload->>'recipient_esp')::int AS recipient_esp, payload->>'is_spam' AS is_spam,
               payload->>'timestamp_created' AS observed_at, payload->>'spf_pass' AS spf_pass,
               payload->>'dkim_pass' AS dkim_pass, payload->>'dmarc_pass' AS dmarc_pass
        FROM instantly_placement_analytics_raw
        WHERE test_id = ${testId}
        ORDER BY analytics_id, fetched_at DESC
      `),
    );
    if (rows.length === 0) return null;
    seeds = rows
      .filter((r) => typeof r.sender_email === "string" && r.sender_email !== "")
      .map((r) =>
        seedOf(String(r.sender_email), {
          receiver: r.receiver_email === null ? null : String(r.receiver_email),
          provider: providerOfEsp(Number(r.recipient_esp)),
          placement: bool(r.is_spam) ? "spam" : "inbox",
          folder: null,
          msInference: null,
          msScl: null,
          dispatchOutcome: "sent",
          dispatchedAt: null,
          observedAt: iso(r.observed_at),
          spfPass: bool(r.spf_pass),
          dkimPass: bool(r.dkim_pass),
          dmarcPass: bool(r.dmarc_pass),
        }),
      );
  }

  seeds = seeds.filter((s) => inScope(scope, s.sender, logins.get(s.sender)));
  const testedAtRow = rowsOf<{ tested_at: unknown }>(
    await db.execute(sql`SELECT max(tested_at) AS tested_at FROM instantly_placement_results WHERE test_id = ${testId}`),
  )[0];
  const source = testSource(testId);
  return {
    testId,
    source,
    testedAt: iso(testedAtRow?.tested_at),
    scope,
    missingSeedsListed: source === "seed",
    ...countSeeds(seeds),
    seeds,
  };
}
