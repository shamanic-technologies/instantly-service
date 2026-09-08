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
// The volume the cap ramps on. Mocked at its own boundary so the IO tests below
// exercise the sweep, not the union query's SQL.
vi.mock("../../src/lib/recent-send-volume", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  fetchRecentDailyVolume: vi.fn(),
}));

import {
  listAccounts,
  setWarmupDailyLimit,
  setDailyLimit,
  setSlowRamp,
} from "../../src/lib/instantly-client";
import { fetchLifecycleByEmail } from "../../src/lib/account-lifecycle-sync";
import { fetchRecentDailyVolume } from "../../src/lib/recent-send-volume";
import {
  selectLifecycleLimitPatches,
  syncLifecycleLimits,
} from "../../src/lib/sync-lifecycle-limits";

const mockListAccounts = vi.mocked(listAccounts);
const mockSetWarmup = vi.mocked(setWarmupDailyLimit);
const mockSetDaily = vi.mocked(setDailyLimit);
const mockSetSlowRamp = vi.mocked(setSlowRamp);
const mockFetchLifecycle = vi.mocked(fetchLifecycleByEmail);
const mockRecentPeaks = vi.mocked(fetchRecentDailyVolume);

// A fixed clock so age-based (slow-ramp) assertions are deterministic.
const asOf = new Date("2026-07-22T00:00:00Z");

/**
 * Run the selector with every account already AT VOLUME, so the ramp is
 * saturated and the lifecycle state's limit is what binds. The ramp gets its own
 * cases below rather than colouring every assertion in this file.
 */
const patchesAtVolume = (accounts: Account[], lc: Map<string, LifecycleView>) =>
  selectLifecycleLimitPatches(
    accounts,
    lc,
    asOf,
    new Map(accounts.map((a) => [a.email as string, 50])),
  );
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

function lifecycle(
  status: string,
  sendTransport: LifecycleView["sendTransport"] = "instantly",
): LifecycleView {
  return {
    status: status as LifecycleView["status"],
    reason: null,
    updatedAt: null,
    sendTransport,
  };
}

describe("selectLifecycleLimitPatches", () => {
  it("in_production: patches only fields that drift from 50/0 (slowRamp null when undatable)", () => {
    const accounts = [
      acct("aligned@x.com", 50, 0), // aligned → no patch
      acct("drift-both@x.com", 45, 10), // both drift
      acct("drift-daily@x.com", 40, 0), // only daily drifts
      acct("drift-warmup@x.com", 50, 5), // only warmup drifts (the old 45/5 target)
    ];
    const lc = new Map<string, LifecycleView>([
      ["aligned@x.com", lifecycle("in_production")],
      ["drift-both@x.com", lifecycle("in_production")],
      ["drift-daily@x.com", lifecycle("in_production")],
      ["drift-warmup@x.com", lifecycle("in_production")],
    ]);
    expect(patchesAtVolume(accounts, lc)).toEqual([
      { email: "drift-both@x.com", warmup: 0, daily: 50, slowRamp: null },
      { email: "drift-daily@x.com", warmup: null, daily: 50, slowRamp: null },
      { email: "drift-warmup@x.com", warmup: 0, daily: null, slowRamp: null },
    ]);
  });

  it("a warmup target of 0 is a REAL patch, not 'no change' (0 vs null)", () => {
    // The sweep encodes "leave it alone" as null, so the in_production warmup
    // target of 0 must survive both the drift check and the `!== null` guard that
    // decides whether to call Instantly. A truthiness check anywhere here would
    // silently leave the whole fleet warming at its old value.
    const accounts = [acct("warming@x.com", 50, 5)];
    const lc = new Map<string, LifecycleView>([["warming@x.com", lifecycle("in_production")]]);
    const patches = patchesAtVolume(accounts, lc);
    expect(patches).toEqual([
      { email: "warming@x.com", warmup: 0, daily: null, slowRamp: null },
    ]);
    expect(patches[0].warmup).not.toBeNull();
  });

  it("in_recovery: enforces 20/30 (the stuck-50/0 and stuck-45/50 cases)", () => {
    const accounts = [
      acct("stuck-a@x.com", 50, 0), // a demoted account → back to 20/30
      acct("stuck-b@x.com", 45, 50), // both drift → 20/30
      acct("ok@x.com", 20, 30), // aligned → no patch
    ];
    const lc = new Map<string, LifecycleView>([
      ["stuck-a@x.com", lifecycle("in_recovery")],
      ["stuck-b@x.com", lifecycle("in_recovery")],
      ["ok@x.com", lifecycle("in_recovery")],
    ]);
    expect(patchesAtVolume(accounts, lc)).toEqual([
      { email: "stuck-a@x.com", warmup: 30, daily: 20, slowRamp: null },
      { email: "stuck-b@x.com", warmup: 30, daily: 20, slowRamp: null },
    ]);
  });

  it("caps a RAMPING account's daily_limit at its measured volume, not the state's full 50", () => {
    // Peaked at 12/day ⇒ may attempt 18. Gmail's real per-user quota tracks what
    // the mailbox has been sending, so the volume ceiling binds before the state
    // one. Slow ramp pre-aligned on both so the assertion isolates `daily`.
    const accounts = [
      acct("ramping@x.com", 50, 0, { timestampCreated: created(90), enableSlowRamp: false }),
      acct("atvolume@x.com", 50, 0, { timestampCreated: created(90), enableSlowRamp: false }),
    ];
    const lc = new Map<string, LifecycleView>([
      ["ramping@x.com", lifecycle("in_production")],
      ["atvolume@x.com", lifecycle("in_production")],
    ]);
    expect(
      selectLifecycleLimitPatches(
        accounts,
        lc,
        asOf,
        new Map([
          ["ramping@x.com", 12],
          ["atvolume@x.com", 40],
        ]),
      ),
    ).toEqual([{ email: "ramping@x.com", warmup: null, daily: 18, slowRamp: null }]);
  });

  it("does NOT re-scale its own output: an already-ramped account is aligned", () => {
    // The ramp is computed off IN_PRODUCTION_DAILY_LIMIT, never off the account's
    // current daily_limit — otherwise each sweep would shrink it again (50→25→13…).
    const accounts = [
      acct("ramping@x.com", 18, 0, { timestampCreated: created(90), enableSlowRamp: false }),
    ];
    const lc = new Map<string, LifecycleView>([["ramping@x.com", lifecycle("in_production")]]);
    expect(
      selectLifecycleLimitPatches(accounts, lc, asOf, new Map([["ramping@x.com", 12]])),
    ).toEqual([]);
  });

  it("in_recovery: the volume ramp binds when it is BELOW the recovery limit", () => {
    // Nothing measured → the floor of 5, under the in_recovery 20 → 5 wins. The
    // mailbox then climbs on its own warmup and placement-test traffic.
    const accounts = [
      acct("quiet@x.com", 20, 30, { timestampCreated: created(90), enableSlowRamp: false }),
    ];
    const lc = new Map<string, LifecycleView>([["quiet@x.com", lifecycle("in_recovery")]]);
    expect(selectLifecycleLimitPatches(accounts, lc, asOf, new Map())).toEqual([
      { email: "quiet@x.com", warmup: null, daily: 5, slowRamp: null },
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
    expect(patchesAtVolume(accounts, lc)).toEqual([
      { email: "byinst@x.com", warmup: null, daily: null, slowRamp: true },
    ]);
  });

  it("slow ramp is age-driven: fresh→true when off, mature→false when on, aligned→skip", () => {
    const accounts = [
      acct("fresh-off@x.com", 50, 0, { enableSlowRamp: false, timestampCreated: created(3) }),
      acct("fresh-on@x.com", 50, 0, { enableSlowRamp: true, timestampCreated: created(3) }), // aligned
      acct("mature-on@x.com", 50, 0, { enableSlowRamp: true, timestampCreated: created(90) }),
      acct("mature-off@x.com", 50, 0, { enableSlowRamp: false, timestampCreated: created(90) }), // aligned
    ];
    const lc = new Map<string, LifecycleView>([
      ["fresh-off@x.com", lifecycle("in_production")],
      ["fresh-on@x.com", lifecycle("in_production")],
      ["mature-on@x.com", lifecycle("in_production")],
      ["mature-off@x.com", lifecycle("in_production")],
    ]);
    // Every account here is at volume, so only slow ramp drifts.
    expect(patchesAtVolume(accounts, lc)).toEqual([
      { email: "fresh-off@x.com", warmup: null, daily: null, slowRamp: true },
      { email: "mature-on@x.com", warmup: null, daily: null, slowRamp: false },
    ]);
  });

  it("treats an absent warmup object as drifting (needs the warmup patch)", () => {
    // `undefined` (Instantly reported no warmup config) is NOT the same as 0
    // (warmup explicitly off) — the former still needs the PATCH.
    const accounts = [acct("nowarmup@x.com", 50, undefined)];
    const lc = new Map<string, LifecycleView>([
      ["nowarmup@x.com", lifecycle("in_production")],
    ]);
    expect(patchesAtVolume(accounts, lc)).toEqual([
      { email: "nowarmup@x.com", warmup: 0, daily: null, slowRamp: null },
    ]);
  });

  it("an smtp account is skipped ENTIRELY — warmup, daily AND slow ramp", () => {
    // Instantly does not dispatch this mailbox: our own worker owns the cap, and
    // the mailbox is frequently one Instantly disabled, so every PATCH here would
    // be both meaningless and likely to fail. Slow ramp included — it is an
    // Instantly campaign setting with no effect on a sequence Instantly never sends.
    const accounts = [
      // Drifts on every single field, and is fresh (so slow ramp would target true).
      acct("self@x.com", 12, 7, { enableSlowRamp: false, timestampCreated: created(3) }),
    ];
    const lc = new Map<string, LifecycleView>([
      ["self@x.com", lifecycle("in_production", "smtp")],
    ]);
    expect(patchesAtVolume(accounts, lc)).toEqual([]);
  });

  it("the SAME drifting account on the instantly transport IS patched", () => {
    // Guards the skip above against becoming a silent blanket no-op.
    const accounts = [
      acct("relay@x.com", 12, 7, { enableSlowRamp: false, timestampCreated: created(3) }),
    ];
    const lc = new Map<string, LifecycleView>([
      ["relay@x.com", lifecycle("in_production", "instantly")],
    ]);
    expect(selectLifecycleLimitPatches(accounts, lc, asOf, new Map())).toEqual([
      { email: "relay@x.com", warmup: 0, daily: 5, slowRamp: true },
    ]);
  });
});

describe("syncLifecycleLimits", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSetWarmup.mockResolvedValue({} as Account);
    mockSetDaily.mockResolvedValue({} as Account);
    mockSetSlowRamp.mockResolvedValue({} as Account);
    // Default: every mailbox already at volume, so the ramp is saturated and
    // these cases exercise the sweep rather than the ramp.
    // Per-day volume, from which the sweep derives each address's own peak.
    const atVolume = (email: string): [string, Map<string, number>] => [
      email,
      // TWO days at 50 — the figure is the second-highest, so one day would read 0.
      new Map([["2026-07-20", 50], ["2026-07-21", 50]]),
    ];
    mockRecentPeaks.mockResolvedValue(
      new Map([
        atVolume("both@x.com"),
        atVolume("aligned@x.com"),
        atVolume("daily@x.com"),
        atVolume("a@x.com"),
        atVolume("b@x.com"),
        atVolume("c@x.com"),
        atVolume("boom@x.com"),
        atVolume("ok@x.com"),
        // `ramp@x.com` is deliberately ABSENT: nothing measured ⇒ the floor.
      ]),
    );
  });

  it("PATCHes drifting fields (warmup/daily/slowRamp), counts field- + account-level totals", async () => {
    mockListAccounts.mockResolvedValue([
      acct("both@x.com", 45, 10), // → warmup 0 + daily 50
      acct("aligned@x.com", 50, 0), // skip
      acct("daily@x.com", 40, 0), // → daily only
      // 3 days old → slowRamp true (age-driven). No measured volume either, so
      // its daily is floored to the 5/day ramp cap.
      acct("ramp@x.com", 50, 0, { enableSlowRamp: false, timestampCreated: created(3) }),
    ]);
    mockFetchLifecycle.mockResolvedValue(
      new Map<string, LifecycleView>([
        ["both@x.com", lifecycle("in_production")],
        ["aligned@x.com", lifecycle("in_production")],
        ["daily@x.com", lifecycle("in_production")],
        ["ramp@x.com", lifecycle("in_production")],
      ]),
    );

    // Pass the fixed clock — slow ramp is still age-driven, so a wall-clock
    // default would make `created(3)` drift further from 3 days every day.
    const summary = await syncLifecycleLimits("key", undefined, asOf);

    expect(mockSetWarmup).toHaveBeenCalledWith("key", "both@x.com", 0);
    expect(mockSetDaily).toHaveBeenCalledWith("key", "both@x.com", 50);
    expect(mockSetDaily).toHaveBeenCalledWith("key", "daily@x.com", 50);
    expect(mockSetDaily).toHaveBeenCalledWith("key", "ramp@x.com", 5);
    expect(mockSetSlowRamp).toHaveBeenCalledTimes(1);
    expect(mockSetSlowRamp).toHaveBeenCalledWith("key", "ramp@x.com", true);
    expect(summary).toEqual({
      accountsRead: 4,
      accountsPatched: 3,
      warmupPatched: 1,
      dailyPatched: 3,
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
    // Both drift on BOTH fields (warmup 10 → 0, daily 45 → 50) so the assertion
    // below can show the daily PATCH being skipped for the account that threw.
    mockListAccounts.mockResolvedValue([
      acct("boom@x.com", 45, 10),
      acct("ok@x.com", 45, 10),
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
    expect(mockSetDaily).toHaveBeenCalledWith("key", "ok@x.com", 50);
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
