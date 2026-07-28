import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Account } from "../../src/lib/instantly-client";
import type { LifecycleView } from "../../src/lib/account-lifecycle-sync";

vi.mock("../../src/lib/instantly-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  listAccounts: vi.fn(),
  setWarmupDailyLimit: vi.fn(),
  setDailyLimit: vi.fn(),
  setSlowRamp: vi.fn(),
}));
vi.mock("../../src/lib/account-lifecycle-sync", () => ({
  fetchLifecycleByEmail: vi.fn(),
}));

import {
  listAccounts,
  setWarmupDailyLimit,
  setDailyLimit,
  setSlowRamp,
} from "../../src/lib/instantly-client";
import { fetchLifecycleByEmail } from "../../src/lib/account-lifecycle-sync";
import {
  selectLifecycleLimitPatches,
  syncLifecycleLimits,
} from "../../src/lib/sync-lifecycle-limits";

const mockListAccounts = vi.mocked(listAccounts);
const mockSetWarmup = vi.mocked(setWarmupDailyLimit);
const mockSetDaily = vi.mocked(setDailyLimit);
const mockSetSlowRamp = vi.mocked(setSlowRamp);
const mockFetchLifecycle = vi.mocked(fetchLifecycleByEmail);

// A fixed clock so age-based (slow-ramp) assertions are deterministic.
const asOf = new Date("2026-07-22T00:00:00Z");
const created = (daysOld: number) =>
  new Date(asOf.getTime() - daysOld * 24 * 60 * 60 * 1000).toISOString();

function acct(
  email: string,
  daily_limit: number | undefined,
  warmupLimit: number | undefined,
  opts: { enableSlowRamp?: boolean; timestampCreated?: string } = {},
): Account {
  return {
    email,
    warmup_status: 0,
    status: 1,
    daily_limit,
    warmup: warmupLimit === undefined ? undefined : { limit: warmupLimit },
    enable_slow_ramp: opts.enableSlowRamp,
    timestamp_created: opts.timestampCreated,
  } as Account;
}

function lifecycle(status: string): LifecycleView {
  return { status: status as LifecycleView["status"], reason: null, updatedAt: null };
}

describe("selectLifecycleLimitPatches", () => {
  it("in_production: patches only fields that drift from 45/5 (slowRamp null when undatable)", () => {
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
    expect(selectLifecycleLimitPatches(accounts, lc, asOf)).toEqual([
      { email: "drift-both@x.com", warmup: 5, daily: 45, slowRamp: null },
      { email: "drift-daily@x.com", warmup: null, daily: 45, slowRamp: null },
      { email: "drift-warmup@x.com", warmup: 5, daily: null, slowRamp: null },
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
    expect(selectLifecycleLimitPatches(accounts, lc, asOf)).toEqual([
      { email: "stuck-a@x.com", warmup: 30, daily: 20, slowRamp: null },
      { email: "stuck-b@x.com", warmup: 30, daily: 20, slowRamp: null },
    ]);
  });

  it("skips deactivated_* / unknown lifecycle for warmup+daily, but STILL enforces age-driven slow ramp", () => {
    // A deactivated account is skipped for warmup/daily (targets null) — but a
    // FRESH one whose slow ramp is off still gets the slow-ramp patch (age-driven,
    // state-independent). An aligned/undatable one drops out entirely.
    const accounts = [
      acct("byinst@x.com", 50, 10, { enableSlowRamp: false, timestampCreated: created(3) }),
      acct("byuser@x.com", 50, 10, { enableSlowRamp: false }), // undatable → slowRamp null → no patch
    ];
    const lc = new Map<string, LifecycleView>([
      ["byinst@x.com", lifecycle("deactivated_by_instantly")],
      ["byuser@x.com", lifecycle("deactivated_by_user")],
    ]);
    expect(selectLifecycleLimitPatches(accounts, lc, asOf)).toEqual([
      { email: "byinst@x.com", warmup: null, daily: null, slowRamp: true },
    ]);
  });

  it("slow ramp is age-driven: fresh→true when off, mature→false when on, aligned→skip", () => {
    const accounts = [
      acct("fresh-off@x.com", 45, 5, { enableSlowRamp: false, timestampCreated: created(3) }),
      acct("fresh-on@x.com", 45, 5, { enableSlowRamp: true, timestampCreated: created(3) }), // aligned
      acct("mature-on@x.com", 45, 5, { enableSlowRamp: true, timestampCreated: created(90) }),
      acct("mature-off@x.com", 45, 5, { enableSlowRamp: false, timestampCreated: created(90) }), // aligned
    ];
    const lc = new Map<string, LifecycleView>([
      ["fresh-off@x.com", lifecycle("in_production")],
      ["fresh-on@x.com", lifecycle("in_production")],
      ["mature-on@x.com", lifecycle("in_production")],
      ["mature-off@x.com", lifecycle("in_production")],
    ]);
    expect(selectLifecycleLimitPatches(accounts, lc, asOf)).toEqual([
      { email: "fresh-off@x.com", warmup: null, daily: null, slowRamp: true },
      { email: "mature-on@x.com", warmup: null, daily: null, slowRamp: false },
    ]);
  });

  it("treats an absent warmup object as drifting (needs the warmup patch)", () => {
    const accounts = [acct("nowarmup@x.com", 45, undefined)];
    const lc = new Map<string, LifecycleView>([
      ["nowarmup@x.com", lifecycle("in_production")],
    ]);
    expect(selectLifecycleLimitPatches(accounts, lc, asOf)).toEqual([
      { email: "nowarmup@x.com", warmup: 5, daily: null, slowRamp: null },
    ]);
  });
});

describe("syncLifecycleLimits", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSetWarmup.mockResolvedValue({} as Account);
    mockSetDaily.mockResolvedValue({} as Account);
    mockSetSlowRamp.mockResolvedValue({} as Account);
  });

  it("PATCHes drifting fields (warmup/daily/slowRamp), counts field- + account-level totals", async () => {
    mockListAccounts.mockResolvedValue([
      acct("both@x.com", 50, 10), // → warmup 5 + daily 45
      acct("aligned@x.com", 45, 5), // skip
      acct("daily@x.com", 40, 5), // → daily only
      acct("ramp@x.com", 45, 5, { enableSlowRamp: false, timestampCreated: created(3) }), // → slowRamp true
    ]);
    mockFetchLifecycle.mockResolvedValue(
      new Map<string, LifecycleView>([
        ["both@x.com", lifecycle("in_production")],
        ["aligned@x.com", lifecycle("in_production")],
        ["daily@x.com", lifecycle("in_production")],
        ["ramp@x.com", lifecycle("in_production")],
      ]),
    );

    const summary = await syncLifecycleLimits("key");

    expect(mockSetWarmup).toHaveBeenCalledWith("key", "both@x.com", 5);
    expect(mockSetDaily).toHaveBeenCalledWith("key", "both@x.com", 45);
    expect(mockSetDaily).toHaveBeenCalledWith("key", "daily@x.com", 45);
    expect(mockSetSlowRamp).toHaveBeenCalledTimes(1);
    expect(mockSetSlowRamp).toHaveBeenCalledWith("key", "ramp@x.com", true);
    expect(summary).toEqual({
      accountsRead: 4,
      accountsPatched: 3,
      warmupPatched: 1,
      dailyPatched: 2,
      slowRampPatched: 1,
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
      slowRampPatched: 0,
      failed: 1,
    });
  });
});
