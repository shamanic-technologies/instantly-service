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
