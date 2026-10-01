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

// The address → real-mailbox map. Empty by default: every address is its own
// login, which is the pre-split behaviour the older cases below pin.
vi.mock("../../src/lib/self-send/mailbox-credentials", () => ({
  loadMailboxLogins: vi.fn(),
}));

import { loadMailboxLogins } from "../../src/lib/self-send/mailbox-credentials";
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
  opts: { enableSlowRamp?: boolean; timestampCreated?: string; status?: number } = {},
): Account {
  return {
    email,
    warmup_status: 0,
    status: opts.status ?? 1,
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

  it("does NOT apply OUR volume ramp on the Instantly transport", () => {
    // Instantly dispatches these mailboxes and throttles them with its own
    // `enable_slow_ramp`. Our volume figure is blind to its warmup pool, so the
    // ramp reads a zero that means "we did not look" and pinned 103-day-old
    // mailboxes scoring 92-100% inbox at 5/day. See `rampAppliesToTransport`.
    const accounts = [
      acct("quiet@x.com", 5, 0, { timestampCreated: created(90), enableSlowRamp: false }),
      acct("atvolume@x.com", 50, 0, { timestampCreated: created(90), enableSlowRamp: false }),
    ];
    const lc = new Map<string, LifecycleView>([
      ["quiet@x.com", lifecycle("in_production")],
      ["atvolume@x.com", lifecycle("in_production")],
    ]);
    expect(
      selectLifecycleLimitPatches(
        accounts,
        lc,
        asOf,
        new Map([
          ["quiet@x.com", 0],
          ["atvolume@x.com", 40],
        ]),
      ),
    ).toEqual([{ email: "quiet@x.com", warmup: null, daily: 50, slowRamp: null }]);
  });

  it("restores a mailbox our own sweep had previously ramped down", () => {
    // The eleven frozen DFY mailboxes: `daily_limit` 5 written by this sweep and
    // re-written every hour. With the ramp gone the state limit is the target.
    const accounts = [
      acct("frozen@x.com", 5, 0, { timestampCreated: created(103), enableSlowRamp: false }),
    ];
    const lc = new Map<string, LifecycleView>([["frozen@x.com", lifecycle("in_production")]]);
    expect(
      selectLifecycleLimitPatches(accounts, lc, asOf, new Map([["frozen@x.com", 1]])),
    ).toEqual([{ email: "frozen@x.com", warmup: null, daily: 50, slowRamp: null }]);
  });

  it("in_recovery on the Instantly transport gets the state's 20/30, un-ramped", () => {
    // Instantly's own warmup is what lifts a recovering mailbox there, and we
    // cannot see that volume — so the state limit is the only honest target.
    const accounts = [
      acct("quiet@x.com", 5, 30, { timestampCreated: created(90), enableSlowRamp: false }),
    ];
    const lc = new Map<string, LifecycleView>([["quiet@x.com", lifecycle("in_recovery")]]);
    expect(selectLifecycleLimitPatches(accounts, lc, asOf, new Map())).toEqual([
      { email: "quiet@x.com", warmup: null, daily: 20, slowRamp: null },
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

  it("smtp + Instantly-ACTIVE in_production at the recovery 20/30 → patched to 50/0, slow ramp untouched", () => {
    // Instantly's warmup pool still sends from an ACTIVE account, and our own
    // cap reads its daily_limit — so both are ours to enforce even on smtp.
    // Slow ramp is a campaign setting Instantly never applies on smtp: untouched.
    const accounts = [
      acct("prod@x.com", 20, 30, { enableSlowRamp: false, timestampCreated: created(3) }),
    ];
    const lc = new Map<string, LifecycleView>([
      ["prod@x.com", lifecycle("in_production", "smtp")],
    ]);
    expect(patchesAtVolume(accounts, lc)).toEqual([
      { email: "prod@x.com", warmup: 0, daily: 50, slowRamp: null },
    ]);
  });

  it("smtp + Instantly-ACTIVE in_recovery at 50/0 → patched to 20/30", () => {
    const accounts = [acct("rec@x.com", 50, 0)];
    const lc = new Map<string, LifecycleView>([
      ["rec@x.com", lifecycle("in_recovery", "smtp")],
    ]);
    expect(patchesAtVolume(accounts, lc)).toEqual([
      { email: "rec@x.com", warmup: 30, daily: 20, slowRamp: null },
    ]);
  });

  it("smtp + Instantly-DISABLED account is skipped ENTIRELY (the PATCH would fail)", () => {
    const accounts = [
      acct("off@x.com", 12, 7, { enableSlowRamp: false, timestampCreated: created(3), status: 0 }),
      acct("err@x.com", 12, 7, { status: -1 }),
    ];
    const lc = new Map<string, LifecycleView>([
      ["off@x.com", lifecycle("in_production", "smtp")],
      ["err@x.com", lifecycle("in_recovery", "smtp")],
    ]);
    expect(patchesAtVolume(accounts, lc)).toEqual([]);
  });

  it("smtp + ACTIVE but deactivated_by_user lifecycle → no limits patch", () => {
    const accounts = [acct("user@x.com", 0, 50)];
    const lc = new Map<string, LifecycleView>([
      ["user@x.com", lifecycle("deactivated_by_user", "smtp")],
    ]);
    expect(patchesAtVolume(accounts, lc)).toEqual([]);
  });

  it("a DISABLED account on the instantly transport is still patched (unchanged behaviour)", () => {
    const accounts = [acct("relayoff@x.com", 20, 30, { status: 0 })];
    const lc = new Map<string, LifecycleView>([
      ["relayoff@x.com", lifecycle("in_production", "instantly")],
    ]);
    expect(patchesAtVolume(accounts, lc)).toEqual([
      { email: "relayoff@x.com", warmup: 0, daily: 50, slowRamp: null },
    ]);
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
      { email: "relay@x.com", warmup: 0, daily: 50, slowRamp: true },
    ]);
  });
});

describe("selectLifecycleLimitPatches — warmup per real mailbox (login)", () => {
  const recoveryAliases = ["a", "b", "c", "d", "e"].map((x) => `${x}@growth.email`);
  const oneLogin = (emails: string[], login: string) =>
    new Map(emails.map((e) => [e, login]));

  it("5 in_recovery aliases on one login split the 30 so the login's sum is ≤ 30", () => {
    const accounts = recoveryAliases.map((e) => acct(e, 20, 30, { status: 1 }));
    const lc = new Map(recoveryAliases.map((e) => [e, lifecycle("in_recovery", "smtp")]));
    const patches = selectLifecycleLimitPatches(
      accounts, lc, asOf, new Map(), oneLogin(recoveryAliases, "kevin@growth.email"),
    );
    expect(patches.map((p) => p.warmup)).toEqual([6, 6, 6, 6, 6]);
    expect(patches.every((p) => p.daily === null)).toBe(true);
    const sum = patches.reduce((t, p) => t + (p.warmup ?? 0), 0);
    expect(sum).toBeLessThanOrEqual(30);
  });

  it("a mixed login (recovery + production aliases) gets 0 warmup on ALL aliases", () => {
    const accounts = [
      acct("r1@salesmolt.com", 20, 30),
      acct("r2@salesmolt.com", 20, 30),
      acct("r3@salesmolt.com", 20, 30),
      acct("p1@salesmolt.com", 50, 30),
      acct("p2@salesmolt.com", 50, 0),
    ];
    const lc = new Map<string, LifecycleView>([
      ["r1@salesmolt.com", lifecycle("in_recovery", "smtp")],
      ["r2@salesmolt.com", lifecycle("in_recovery", "smtp")],
      ["r3@salesmolt.com", lifecycle("in_recovery", "smtp")],
      ["p1@salesmolt.com", lifecycle("in_production", "smtp")],
      ["p2@salesmolt.com", lifecycle("in_production", "smtp")],
    ]);
    const logins = oneLogin(accounts.map((a) => a.email as string), "eric@salesmolt.com");
    expect(selectLifecycleLimitPatches(accounts, lc, asOf, new Map(), logins)).toEqual([
      { email: "r1@salesmolt.com", warmup: 0, daily: null, slowRamp: null },
      { email: "r2@salesmolt.com", warmup: 0, daily: null, slowRamp: null },
      { email: "r3@salesmolt.com", warmup: 0, daily: null, slowRamp: null },
      { email: "p1@salesmolt.com", warmup: 0, daily: null, slowRamp: null },
    ]);
  });

  it("an in_production alias Instantly has DISABLED still zeroes its recovery siblings", () => {
    // We dispatch outreach from it ourselves, so it spends the login's 50.
    const accounts = [acct("r@m.com", 20, 30), acct("p@m.com", 50, 0, { status: 0 })];
    const lc = new Map<string, LifecycleView>([
      ["r@m.com", lifecycle("in_recovery", "smtp")],
      ["p@m.com", lifecycle("in_production", "smtp")],
    ]);
    const logins = oneLogin(["r@m.com", "p@m.com"], "login@m.com");
    expect(selectLifecycleLimitPatches(accounts, lc, asOf, new Map(), logins)).toEqual([
      { email: "r@m.com", warmup: 0, daily: null, slowRamp: null },
    ]);
  });

  it("single-alias mailboxes are unchanged (map present or absent)", () => {
    const accounts = [acct("solo@x.com", 20, 5), acct("prod@x.com", 50, 10)];
    const lc = new Map<string, LifecycleView>([
      ["solo@x.com", lifecycle("in_recovery", "smtp")],
      ["prod@x.com", lifecycle("in_production", "smtp")],
    ]);
    const expected = [
      { email: "solo@x.com", warmup: 30, daily: null, slowRamp: null },
      { email: "prod@x.com", warmup: 0, daily: null, slowRamp: null },
    ];
    expect(selectLifecycleLimitPatches(accounts, lc, asOf, new Map())).toEqual(expected);
    expect(
      selectLifecycleLimitPatches(
        accounts, lc, asOf, new Map(),
        new Map([["solo@x.com", "solo@x.com"], ["prod@x.com", "prod@x.com"]]),
      ),
    ).toEqual(expected);
  });

  it("an Instantly-DISABLED smtp alias is skipped, and the warmup it still HOLDS comes off the split", () => {
    const accounts = [
      acct("on1@g.com", 20, 30),
      acct("on2@g.com", 20, 30),
      acct("off@g.com", 20, 10, { status: -1 }),
    ];
    const lc = new Map(accounts.map((a) => [a.email as string, lifecycle("in_recovery", "smtp")]));
    const logins = oneLogin(accounts.map((a) => a.email as string), "kevin@g.com");
    expect(selectLifecycleLimitPatches(accounts, lc, asOf, new Map(), logins)).toEqual([
      { email: "on1@g.com", warmup: 10, daily: null, slowRamp: null },
      { email: "on2@g.com", warmup: 10, daily: null, slowRamp: null },
    ]);
  });

  it("a disabled alias holding MORE than the login's budget leaves its siblings at 0, never negative", () => {
    const accounts = [acct("on@h.com", 20, 30), acct("off@h.com", 20, 30, { status: -3 })];
    const lc = new Map(accounts.map((a) => [a.email as string, lifecycle("in_recovery", "smtp")]));
    const logins = oneLogin(accounts.map((a) => a.email as string), "kevin@h.com");
    expect(selectLifecycleLimitPatches(accounts, lc, asOf, new Map(), logins)).toEqual([
      { email: "on@h.com", warmup: 0, daily: null, slowRamp: null },
    ]);
  });

  it("an alias flapping disabled ↔ active keeps the login ≤ 30 in BOTH states and stops oscillating (prod 2026-10-01)", () => {
    // growthagency.email: 5 aliases, one flapping -3 ↔ 1. Ignoring the held
    // warmup split 30/4 = 7 while it was off and it came back at 6: 4×7 + 6 = 34.
    const emails = ["kevin", "kevinl", "kevin.lourd", "klourd", "lourd"].map((x) => `${x}@ga.email`);
    const lc = new Map(emails.map((e) => [e, lifecycle("in_recovery", "smtp")]));
    const logins = oneLogin(emails, "kevin@ga.email");
    const sumAfter = (accounts: ReturnType<typeof acct>[]) => {
      const patched = new Map(
        selectLifecycleLimitPatches(accounts, lc, asOf, new Map(), logins).map((p) => [p.email, p.warmup]),
      );
      return accounts.reduce((t, a) => t + (patched.get(a.email as string) ?? a.warmup?.limit ?? 0), 0);
    };
    // The prod state: kevin@ disabled holding 6, its siblings at 7.
    const flappedOff = emails.map((e) => acct(e, 20, e.startsWith("kevin@") ? 6 : 7, e.startsWith("kevin@") ? { status: -3 } : {}));
    expect(selectLifecycleLimitPatches(flappedOff, lc, asOf, new Map(), logins).map((p) => p.warmup)).toEqual([6, 6, 6, 6]);
    expect(sumAfter(flappedOff)).toBe(30);
    // Instantly resumes it: every alias already at 6, nothing to patch, still 30.
    const resumed = emails.map((e) => acct(e, 20, 6, { status: 1 }));
    expect(selectLifecycleLimitPatches(resumed, lc, asOf, new Map(), logins)).toEqual([]);
    expect(sumAfter(resumed)).toBe(30);
  });

  it("deactivated aliases on a shared login are untouched and not counted", () => {
    const accounts = [acct("r@d.com", 20, 30), acct("u@d.com", 20, 50)];
    const lc = new Map<string, LifecycleView>([
      ["r@d.com", lifecycle("in_recovery", "smtp")],
      ["u@d.com", lifecycle("deactivated_by_user", "smtp")],
    ]);
    const logins = oneLogin(["r@d.com", "u@d.com"], "l@d.com");
    expect(selectLifecycleLimitPatches(accounts, lc, asOf, new Map(), logins)).toEqual([]);
  });

  it("matches the login case-insensitively", () => {
    const accounts = [acct("A@Case.com", 20, 30), acct("b@case.com", 20, 30)];
    const lc = new Map<string, LifecycleView>([
      ["A@Case.com", lifecycle("in_recovery", "smtp")],
      ["b@case.com", lifecycle("in_recovery", "smtp")],
    ]);
    const logins = oneLogin(["a@case.com", "b@case.com"], "l@case.com");
    expect(
      selectLifecycleLimitPatches(accounts, lc, asOf, new Map(), logins).map((p) => p.warmup),
    ).toEqual([15, 15]);
  });
});

describe("syncLifecycleLimits", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(loadMailboxLogins).mockResolvedValue(new Map());
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
      // 3 days old → slowRamp true (age-driven). Its daily is already the state's
      // 50 and OUR ramp no longer touches the Instantly transport, so no daily patch.
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
    expect(mockSetDaily).not.toHaveBeenCalledWith("key", "ramp@x.com", 5);
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
