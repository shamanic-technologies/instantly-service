import { describe, it, expect, vi, beforeEach } from "vitest";
import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";

// Mock the DB — every fn under test bottoms out at db.execute(sql`...`).
const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockExecute(...args) },
}));

import {
  fetchQueueSizeByAccount,
  fetchSentYesterdayByAccount,
  fetchQueueBreakdownByAccount,
  fetchAccountLoad,
  fetchAccountLoadCached,
} from "../../src/lib/account-sending-stats";
import { clearStatsCache } from "../../src/lib/stats-cache";

const dialect = new PgDialect();
/** Compile the SQL object handed to the Nth db.execute call into raw text. */
function executedSqlText(callIndex: number): string {
  const arg = mockExecute.mock.calls[callIndex]?.[0] as SQL;
  return dialect.sqlToQuery(arg).sql;
}

beforeEach(() => {
  mockExecute.mockReset();
  clearStatsCache();
});

describe("fetchQueueSizeByAccount — persisted-account attribution", () => {
  it("attributes queue via COALESCE(persisted account_email, observed) with a LEFT JOIN", async () => {
    mockExecute.mockResolvedValueOnce([]);
    await fetchQueueSizeByAccount();
    const text = executedSqlText(0).toLowerCase();
    // Persisted column drives attribution, falling back to the observed send.
    expect(text).toContain("persisted_account");
    expect(text).toContain("coalesce");
    // LEFT JOIN keeps a persisted-but-not-yet-sent campaign in the result
    // (the INNER JOIN used to drop it — the over-concentration gap).
    expect(text).toContain("left join");
    expect(text).not.toMatch(/\binner join\b/);
  });
});

describe("fetchSentYesterdayByAccount — previous full UTC day", () => {
  it("counts real email_sent events bounded to [prev-midnight, today-midnight)", async () => {
    mockExecute.mockResolvedValueOnce([
      { account_email: "a@x.com", count: 7 },
    ]);
    const map = await fetchSentYesterdayByAccount();
    expect(map.get("a@x.com")).toBe(7);

    const text = executedSqlText(0).toLowerCase();
    // Same provenance/filters as sentToday — real dispatches only.
    expect(text).toContain("event_type");
    expect(text).toContain("'email_sent'");
    expect(text).toContain("inferred = false");
    // Window: >= yesterday-midnight AND < today-midnight (excludes today + older).
    expect(text).toContain("date_trunc('day'");
    expect(text).toContain("interval '1 day'");
    expect(text).toMatch(/timestamp\s+<\s+date_trunc/);
  });

  it("returns an empty map (honest 0 upstream) when no account sent yesterday", async () => {
    mockExecute.mockResolvedValueOnce([]);
    const map = await fetchSentYesterdayByAccount();
    expect(map.size).toBe(0);
  });
});

describe("fetchQueueBreakdownByAccount — per-STEP partition", () => {
  it("chains real bronze delays across every step, attributes via COALESCE, partitions per account", async () => {
    const asOf = new Date("2026-07-11T12:00:00.000Z");
    const DAY = 86_400_000;
    mockExecute.mockResolvedValueOnce([
      // never-sent, 2 un-sent steps → both firstUnsent
      {
        account_email: "a@x.com",
        last_sent_step: null,
        last_sent_at: null,
        provisioned_steps: [1, 2],
        step_config: null,
      },
      // sent 3d ago at step 1; steps 2,3 queued; delays [3,7]:
      // step2 = +3 → today; step3 = +10 → later.
      {
        account_email: "a@x.com",
        last_sent_step: 1,
        last_sent_at: new Date(asOf.getTime() - 3 * DAY).toISOString(),
        provisioned_steps: [2, 3],
        step_config: [{ delay: 3 }, { delay: 7 }],
      },
      // b@x.com: sent today at step 2; step 3 queued; delay steps[1]=9 → later.
      {
        account_email: "b@x.com",
        last_sent_step: 2,
        last_sent_at: new Date(asOf.getTime()).toISOString(),
        provisioned_steps: [3],
        step_config: [{ delay: 1 }, { delay: 9 }],
      },
    ]);

    const map = await fetchQueueBreakdownByAccount(asOf);
    // a@x.com: 2 sequences, 4 steps → 2 firstUnsent + 1 today + 0 tomorrow + 1 later.
    expect(map.get("a@x.com")).toEqual({
      sequences: 2,
      steps: 4,
      firstUnsent: 2,
      nextToday: 1,
      nextTomorrow: 0,
      nextLater: 1,
    });
    // Invariant: the four buckets sum to STEPS (not sequences).
    const a = map.get("a@x.com")!;
    expect(a.firstUnsent + a.nextToday + a.nextTomorrow + a.nextLater).toBe(a.steps);
    expect(map.get("b@x.com")).toEqual({
      sequences: 1,
      steps: 1,
      firstUnsent: 0,
      nextToday: 0,
      nextTomorrow: 0,
      nextLater: 1,
    });

    const text = executedSqlText(0).toLowerCase();
    // Same queued gate + COALESCE attribution as fetchQueueSizeByAccount.
    expect(text).toContain("coalesce");
    expect(text).toContain("delivery_status in ('contacted', 'sent')");
    // Loads the distinct provisioned step numbers (same set queueSize counts).
    expect(text).toContain("array_agg(distinct sc.step) filter (where sc.status = 'provisioned')");
    // Full per-step delay array from the latest bronze sequence config (chained).
    expect(text).toContain("instantly_campaigns_config_raw");
    expect(text).toContain("'sequences'");
    expect(text).toContain("->'steps'");
    expect(text).toContain("order by r.fetched_at desc");
  });
});

describe("fetchAccountLoad — merge sentToday + queueSize", () => {
  it("sums per-account load across both maps", async () => {
    // Array-literal order: fetchSentTodayByAccount() is invoked first, so its
    // db.execute is queued first, then fetchQueueSizeByAccount().
    mockExecute
      .mockResolvedValueOnce([
        { account_email: "a@x.com", count: 5 },
        { account_email: "b@x.com", count: 1 },
      ]) // sentToday
      .mockResolvedValueOnce([
        { account_email: "a@x.com", count: 3 },
        { account_email: "c@x.com", count: 7 },
      ]); // queueSize

    const load = await fetchAccountLoad();
    expect(load.get("a@x.com")).toBe(8); // 5 sent + 3 queued
    expect(load.get("b@x.com")).toBe(1); // sent only
    expect(load.get("c@x.com")).toBe(7); // queued only
  });
});

describe("fetchAccountLoadCached — 60s TTL cache", () => {
  it("collapses a burst to a single load snapshot within the window", async () => {
    mockExecute
      .mockResolvedValueOnce([{ account_email: "a@x.com", count: 4 }]) // sent
      .mockResolvedValueOnce([{ account_email: "a@x.com", count: 2 }]); // queue

    const first = await fetchAccountLoadCached();
    const second = await fetchAccountLoadCached();

    expect(first.get("a@x.com")).toBe(6);
    expect(second.get("a@x.com")).toBe(6);
    // Two db.execute calls total (sent + queue), NOT four — the second cached
    // read hits the in-memory snapshot, not the DB.
    expect(mockExecute.mock.calls.length).toBe(2);
  });

  it("re-fetches after the cache is cleared", async () => {
    mockExecute.mockResolvedValue([]);
    await fetchAccountLoadCached();
    clearStatsCache();
    await fetchAccountLoadCached();
    expect(mockExecute.mock.calls.length).toBe(4); // 2 per uncached snapshot
  });
});
