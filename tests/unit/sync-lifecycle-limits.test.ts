import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Account } from "../../src/lib/instantly-client";
import type { LifecycleView } from "../../src/lib/account-lifecycle-sync";

vi.mock("../../src/lib/instantly-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  listAccounts: vi.fn(),
  setWarmupDailyLimit: vi.fn(),
  setDailyLimit: vi.fn(),
}));
vi.mock("../../src/lib/account-lifecycle-sync", () => ({
  fetchLifecycleByEmail: vi.fn(),
}));

import {
  listAccounts,
  setWarmupDailyLimit,
  setDailyLimit,
} from "../../src/lib/instantly-client";
import { fetchLifecycleByEmail } from "../../src/lib/account-lifecycle-sync";
import {
  selectLifecycleLimitPatches,
  syncLifecycleLimits,
} from "../../src/lib/sync-lifecycle-limits";

const mockListAccounts = vi.mocked(listAccounts);
const mockSetWarmup = vi.mocked(setWarmupDailyLimit);
const mockSetDaily = vi.mocked(setDailyLimit);
const mockFetchLifecycle = vi.mocked(fetchLifecycleByEmail);

function acct(
  email: string,
  daily_limit: number | undefined,
  warmupLimit: number | undefined,
): Account {
  return {
    email,
    warmup_status: 0,
    status: 1,
    daily_limit,
    warmup: warmupLimit === undefined ? undefined : { limit: warmupLimit },
  } as Account;
}

function lifecycle(status: string): LifecycleView {
  return { status: status as LifecycleView["status"], reason: null, updatedAt: null };
}

describe("selectLifecycleLimitPatches", () => {
  it("in_production: patches only fields that drift from 45/5", () => {
    const accounts = [
      acct("aligned@x.com", 45, 5), // aligned → no patch
      acct("drift-both@x.com", 50, 10), // magnolia case → both drift
      acct("drift-daily@x.com", 40, 5), // only daily drifts
      acct("drift-warmup@x.com", 45, 50), // only warmup drifts
    ];
    const lc = new Map<string, LifecycleView>([
      ["aligned@x.com", lifecycle("in_production")],
      ["drift-both@x.com", lifecycle("in_production")],
      ["drift-daily@x.com", lifecycle("in_production")],
      ["drift-warmup@x.com", lifecycle("in_production")],
    ]);
    expect(selectLifecycleLimitPatches(accounts, lc)).toEqual([
      { email: "drift-both@x.com", warmup: 5, daily: 45 },
      { email: "drift-daily@x.com", warmup: null, daily: 45 },
      { email: "drift-warmup@x.com", warmup: 5, daily: null },
    ]);
  });

  it("in_recovery: enforces 20/30 (the stuck-45/5 and stuck-45/50 cases)", () => {
    const accounts = [
      acct("stuck-a@x.com", 45, 5), // both drift → 20/30
      acct("stuck-b@x.com", 45, 50), // both drift → 20/30
      acct("ok@x.com", 20, 30), // aligned → no patch
    ];
    const lc = new Map<string, LifecycleView>([
      ["stuck-a@x.com", lifecycle("in_recovery")],
      ["stuck-b@x.com", lifecycle("in_recovery")],
      ["ok@x.com", lifecycle("in_recovery")],
    ]);
    expect(selectLifecycleLimitPatches(accounts, lc)).toEqual([
      { email: "stuck-a@x.com", warmup: 30, daily: 20 },
      { email: "stuck-b@x.com", warmup: 30, daily: 20 },
    ]);
  });

  it("skips deactivated_* and unknown/absent lifecycle (targets are null)", () => {
    const accounts = [
      acct("byinst@x.com", 50, 10),
      acct("byuser@x.com", 50, 10),
      acct("unclassified@x.com", 50, 10),
    ];
    const lc = new Map<string, LifecycleView>([
      ["byinst@x.com", lifecycle("deactivated_by_instantly")],
      ["byuser@x.com", lifecycle("deactivated_by_user")],
      // unclassified@x.com absent from the map
    ]);
    expect(selectLifecycleLimitPatches(accounts, lc)).toEqual([]);
  });

  it("treats an absent warmup object as drifting (needs the warmup patch)", () => {
    const accounts = [acct("nowarmup@x.com", 45, undefined)];
    const lc = new Map<string, LifecycleView>([
      ["nowarmup@x.com", lifecycle("in_production")],
    ]);
    expect(selectLifecycleLimitPatches(accounts, lc)).toEqual([
      { email: "nowarmup@x.com", warmup: 5, daily: null },
    ]);
  });
});

describe("syncLifecycleLimits", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSetWarmup.mockResolvedValue({} as Account);
    mockSetDaily.mockResolvedValue({} as Account);
  });

  it("PATCHes drifting fields, counts field-level + account-level totals", async () => {
    mockListAccounts.mockResolvedValue([
      acct("both@x.com", 50, 10), // → warmup 5 + daily 45
      acct("aligned@x.com", 45, 5), // skip
      acct("daily@x.com", 40, 5), // → daily only
    ]);
    mockFetchLifecycle.mockResolvedValue(
      new Map<string, LifecycleView>([
        ["both@x.com", lifecycle("in_production")],
        ["aligned@x.com", lifecycle("in_production")],
        ["daily@x.com", lifecycle("in_production")],
      ]),
    );

    const summary = await syncLifecycleLimits("key");

    expect(mockSetWarmup).toHaveBeenCalledTimes(1);
    expect(mockSetWarmup).toHaveBeenCalledWith("key", "both@x.com", 5);
    expect(mockSetDaily).toHaveBeenCalledTimes(2);
    expect(mockSetDaily).toHaveBeenCalledWith("key", "both@x.com", 45);
    expect(mockSetDaily).toHaveBeenCalledWith("key", "daily@x.com", 45);
    expect(summary).toEqual({
      accountsRead: 3,
      accountsPatched: 2,
      warmupPatched: 1,
      dailyPatched: 2,
      failed: 0,
    });
  });

  it("bounds the batch by limit", async () => {
    mockListAccounts.mockResolvedValue([
      acct("a@x.com", 50, 10),
      acct("b@x.com", 50, 10),
      acct("c@x.com", 50, 10),
    ]);
    mockFetchLifecycle.mockResolvedValue(
      new Map<string, LifecycleView>([
        ["a@x.com", lifecycle("in_production")],
        ["b@x.com", lifecycle("in_production")],
        ["c@x.com", lifecycle("in_production")],
      ]),
    );

    const summary = await syncLifecycleLimits("key", 2);

    expect(summary.accountsPatched).toBe(2);
    expect(summary.accountsRead).toBe(3);
  });

  it("fails loud per account: a warmup PATCH error skips that account's daily + counts failed", async () => {
    mockListAccounts.mockResolvedValue([
      acct("boom@x.com", 50, 10),
      acct("ok@x.com", 50, 10),
    ]);
    mockFetchLifecycle.mockResolvedValue(
      new Map<string, LifecycleView>([
        ["boom@x.com", lifecycle("in_production")],
        ["ok@x.com", lifecycle("in_production")],
      ]),
    );
    mockSetWarmup
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValueOnce({} as Account);

    const summary = await syncLifecycleLimits("key");

    // boom's daily PATCH is skipped (warmup threw first); ok patches both.
    expect(mockSetDaily).toHaveBeenCalledTimes(1);
    expect(mockSetDaily).toHaveBeenCalledWith("key", "ok@x.com", 45);
    expect(summary).toEqual({
      accountsRead: 2,
      accountsPatched: 1,
      warmupPatched: 1,
      dailyPatched: 1,
      failed: 1,
    });
  });
});
