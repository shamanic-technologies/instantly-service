import { describe, it, expect } from "vitest";
import { buildAccountHealth, mapProviderCode } from "../../src/lib/account-health";
import type { Account } from "../../src/lib/instantly-client";
import type { LifecycleView } from "../../src/lib/account-lifecycle-sync";
import type { LifecycleStatus } from "../../src/lib/account-lifecycle";

function acc(overrides: Partial<Account> & { email: string }): Account {
  return {
    warmup_status: 1,
    status: 1,
    stat_warmup_score: 100,
    daily_limit: 30,
    ...overrides,
  };
}

function lifecycle(
  status: LifecycleStatus,
  reason: string | null = null,
  updatedAt: string | null = "2026-07-05T00:00:00.000Z",
): LifecycleView {
  return { status, reason, updatedAt };
}

describe("buildAccountHealth", () => {
  it("maps an in_production account to the locked shape, all scalars typed, placement null", () => {
    const [row] = buildAccountHealth(
      [acc({ email: "jane@send-domain.com", stat_warmup_score: 100, daily_limit: 40 })],
      new Map(),
      new Map(),
      new Map(),
      new Map([["jane@send-domain.com", lifecycle("in_production", "passed")]]),
    );

    expect(row).toEqual({
      email: "jane@send-domain.com",
      domain: "send-domain.com",
      status: "active",
      warmupScore: 100,
      dailyLimit: 40,
      warmupLimit: null,
      blocked: false,
      blockReason: null,
      lifecycleStatus: "in_production",
      lifecycleReason: "passed",
      lifecycleUpdatedAt: "2026-07-05T00:00:00.000Z",
      inboxPlacement: null,
      sentToday: 0,
      sentYesterday: 0,
      queueSize: 0,
      queuedSequences: 0,
      queuedFirstUnsent: 0,
      queuedNextToday: 0,
      queuedNextTomorrow: 0,
      queuedNextLater: 0,
      accountType: null,
    });
  });

  it("in_production lifecycle → not blocked", () => {
    const [row] = buildAccountHealth(
      [acc({ email: "a@good.com" })],
      new Map(),
      new Map(),
      new Map(),
      new Map([["a@good.com", lifecycle("in_production", "passed")]]),
    );
    expect(row.blocked).toBe(false);
    expect(row.blockReason).toBeNull();
    expect(row.lifecycleStatus).toBe("in_production");
  });

  it("in_recovery lifecycle → blocked with reason 'in_recovery'", () => {
    const [row] = buildAccountHealth(
      [acc({ email: "r@good.com" })],
      new Map(),
      new Map(),
      new Map(),
      new Map([["r@good.com", lifecycle("in_recovery", "low placement")]]),
    );
    expect(row.blocked).toBe(true);
    expect(row.blockReason).toBe("in_recovery");
    expect(row.lifecycleStatus).toBe("in_recovery");
    expect(row.lifecycleReason).toBe("low placement");
  });

  it("deactivated_by_user lifecycle → blocked with reason 'deactivated_by_user'", () => {
    const [row] = buildAccountHealth(
      [acc({ email: "u@good.com" })],
      new Map(),
      new Map(),
      new Map(),
      new Map([["u@good.com", lifecycle("deactivated_by_user")]]),
    );
    expect(row.blocked).toBe(true);
    expect(row.blockReason).toBe("deactivated_by_user");
    expect(row.lifecycleStatus).toBe("deactivated_by_user");
  });

  it("deactivated_by_instantly lifecycle → blocked with reason 'deactivated_by_instantly'", () => {
    const [row] = buildAccountHealth(
      [acc({ email: "i@good.com" })],
      new Map(),
      new Map(),
      new Map(),
      new Map([["i@good.com", lifecycle("deactivated_by_instantly")]]),
    );
    expect(row.blocked).toBe(true);
    expect(row.blockReason).toBe("deactivated_by_instantly");
    expect(row.lifecycleStatus).toBe("deactivated_by_instantly");
  });

  it("account absent from the lifecycle map → blocked 'unclassified', null lifecycle fields", () => {
    const [row] = buildAccountHealth(
      [acc({ email: "n@good.com" })],
      new Map(),
      new Map(),
      new Map(),
      new Map(),
    );
    expect(row.blocked).toBe(true);
    expect(row.blockReason).toBe("unclassified");
    expect(row.lifecycleStatus).toBeNull();
    expect(row.lifecycleReason).toBeNull();
    expect(row.lifecycleUpdatedAt).toBeNull();
  });

  it("no lifecycle map at all → every account blocked 'unclassified'", () => {
    const [row] = buildAccountHealth([acc({ email: "z@good.com" })]);
    expect(row.blocked).toBe(true);
    expect(row.blockReason).toBe("unclassified");
    expect(row.lifecycleStatus).toBeNull();
  });

  it("uses null (not 0) for genuinely-unknown warmupScore / dailyLimit", () => {
    const [row] = buildAccountHealth([
      { email: "z@good.com", warmup_status: 1, status: 1 } as Account,
    ]);
    expect(row.warmupScore).toBeNull();
    expect(row.dailyLimit).toBeNull();
  });

  it("returns null domain for a malformed email (no @)", () => {
    const [row] = buildAccountHealth([acc({ email: "nodomain" })]);
    expect(row.domain).toBeNull();
  });

  it("returns [] for an empty account list", () => {
    expect(buildAccountHealth([])).toEqual([]);
  });

  it("inboxPlacement is null when no placement map is provided (never tested)", () => {
    const rows = buildAccountHealth([
      acc({ email: "a@good.com" }),
      acc({ email: "b@distribute.you" }),
      acc({ email: "c@good.com", status: 0 }),
    ]);
    expect(rows.every((r) => r.inboxPlacement === null)).toBe(true);
  });

  it("injects placement from the map, null for accounts absent from it", () => {
    const placement = new Map([
      ["a@good.com", { inboxPct: 82, spamPct: 12, missingPct: 6, testedAt: "2026-06-30T09:00:00.000Z" }],
    ]);
    const rows = buildAccountHealth(
      [acc({ email: "a@good.com" }), acc({ email: "b@good.com" })],
      placement,
    );
    const byEmail = Object.fromEntries(rows.map((r) => [r.email, r]));
    expect(byEmail["a@good.com"].inboxPlacement).toEqual({
      inboxPct: 82,
      spamPct: 12,
      missingPct: 6,
      testedAt: "2026-06-30T09:00:00.000Z",
    });
    expect(byEmail["b@good.com"].inboxPlacement).toBeNull();
  });

  it("injects sentToday / queueSize from their maps, 0 when absent (never fabricated)", () => {
    const sent = new Map([["a@good.com", 4]]);
    const queue = new Map([["a@good.com", 12]]);
    const rows = buildAccountHealth(
      [acc({ email: "a@good.com" }), acc({ email: "b@good.com" })],
      new Map(),
      sent,
      queue,
    );
    const byEmail = Object.fromEntries(rows.map((r) => [r.email, r]));
    expect(byEmail["a@good.com"].sentToday).toBe(4);
    expect(byEmail["a@good.com"].queueSize).toBe(12);
    // Absent from both maps → honest 0, not fabricated.
    expect(byEmail["b@good.com"].sentToday).toBe(0);
    expect(byEmail["b@good.com"].queueSize).toBe(0);
  });

  it("derives warmupLimit from warmup.limit, DISTINCT from dailyLimit", () => {
    const [row] = buildAccountHealth([
      acc({
        email: "amy@dfy.com",
        daily_limit: 50,
        warmup: { limit: 10 },
      }),
    ]);
    // The two send-config numbers are independent and provably distinct.
    expect(row.dailyLimit).toBe(50);
    expect(row.warmupLimit).toBe(10);
    expect(row.warmupLimit).not.toBe(row.dailyLimit);
  });

  it("warmupLimit is null when Instantly reports no warmup config", () => {
    const [row] = buildAccountHealth([
      { email: "z@good.com", warmup_status: 1, status: 1, daily_limit: 30 } as Account,
    ]);
    expect(row.warmupLimit).toBeNull();
    // dailyLimit still populated — the two are independent.
    expect(row.dailyLimit).toBe(30);
  });

  it("injects sentYesterday from the 6th-arg map, honest 0 when absent", () => {
    const sentYesterday = new Map([["a@good.com", 7]]);
    const rows = buildAccountHealth(
      [acc({ email: "a@good.com" }), acc({ email: "b@good.com" })],
      new Map(),
      new Map(),
      new Map(),
      new Map(),
      sentYesterday,
    );
    const byEmail = Object.fromEntries(rows.map((r) => [r.email, r]));
    expect(byEmail["a@good.com"].sentYesterday).toBe(7);
    // Absent from the map → honest 0, not fabricated.
    expect(byEmail["b@good.com"].sentYesterday).toBe(0);
  });

  it("injects the queue breakdown (7th-arg map); STEP-partition invariant holds; queueSize === bucket sum; honest 0 when absent", () => {
    // 8 queued STEPS across 5 sequences: buckets sum to steps, NOT to sequences.
    const breakdown = new Map([
      [
        "a@good.com",
        { sequences: 5, steps: 8, firstUnsent: 3, nextToday: 3, nextTomorrow: 0, nextLater: 2 },
      ],
    ]);
    const rows = buildAccountHealth(
      [acc({ email: "a@good.com" }), acc({ email: "b@good.com" })],
      new Map(),
      new Map(),
      new Map(),
      new Map(),
      new Map(),
      breakdown,
    );
    const byEmail = Object.fromEntries(rows.map((r) => [r.email, r]));
    const a = byEmail["a@good.com"];
    expect(a.queuedSequences).toBe(5);
    expect(a.queuedFirstUnsent).toBe(3);
    expect(a.queuedNextToday).toBe(3);
    expect(a.queuedNextTomorrow).toBe(0);
    expect(a.queuedNextLater).toBe(2);
    // queueSize is sourced from the breakdown's step total → equals the bucket sum.
    expect(a.queueSize).toBe(8);
    // The four buckets PARTITION the queued STEPS (== queueSize), not sequences.
    expect(
      a.queuedFirstUnsent + a.queuedNextToday + a.queuedNextTomorrow + a.queuedNextLater,
    ).toBe(a.queueSize);
    expect(a.queueSize).not.toBe(a.queuedSequences);
    // Absent from the map → all honest 0 (including queueSize).
    const b = byEmail["b@good.com"];
    expect([b.queueSize, b.queuedSequences, b.queuedFirstUnsent, b.queuedNextToday, b.queuedNextTomorrow, b.queuedNextLater]).toEqual([0, 0, 0, 0, 0, 0]);
  });

  it("lifecycle status drives blocked per account, mixed fleet", () => {
    const rows = buildAccountHealth(
      [
        acc({ email: "prod@good.com" }),
        acc({ email: "rec@good.com" }),
        acc({ email: "off@good.com" }),
        acc({ email: "none@good.com" }),
      ],
      new Map(),
      new Map(),
      new Map(),
      new Map<string, LifecycleView>([
        ["prod@good.com", lifecycle("in_production")],
        ["rec@good.com", lifecycle("in_recovery")],
        ["off@good.com", lifecycle("deactivated_by_user")],
      ]),
    );
    const byEmail = Object.fromEntries(rows.map((r) => [r.email, r]));
    expect(byEmail["prod@good.com"].blocked).toBe(false);
    expect(byEmail["rec@good.com"].blockReason).toBe("in_recovery");
    expect(byEmail["off@good.com"].blockReason).toBe("deactivated_by_user");
    expect(byEmail["none@good.com"].blockReason).toBe("unclassified");
  });

  it("maps provider_code to accountType (google/microsoft/imap), null otherwise", () => {
    const rows = buildAccountHealth([
      acc({ email: "g@good.com", provider_code: 1 }),
      acc({ email: "m@good.com", provider_code: 2 }),
      acc({ email: "i@good.com", provider_code: 3 }),
      acc({ email: "j@good.com", provider_code: 4 }),
      acc({ email: "n@good.com" }),
      acc({ email: "u@good.com", provider_code: 99 }),
    ]);
    const byEmail = Object.fromEntries(rows.map((r) => [r.email, r.accountType]));
    expect(byEmail["g@good.com"]).toBe("google");
    expect(byEmail["m@good.com"]).toBe("microsoft");
    expect(byEmail["i@good.com"]).toBe("imap");
    expect(byEmail["j@good.com"]).toBe("imap");
    expect(byEmail["n@good.com"]).toBeNull();
    expect(byEmail["u@good.com"]).toBeNull();
  });
});

describe("mapProviderCode", () => {
  it("maps known codes and returns null for unknown/absent", () => {
    expect(mapProviderCode(1)).toBe("google");
    expect(mapProviderCode(2)).toBe("microsoft");
    expect(mapProviderCode(3)).toBe("imap");
    expect(mapProviderCode(4)).toBe("imap");
    expect(mapProviderCode(0)).toBeNull();
    expect(mapProviderCode(undefined)).toBeNull();
  });
});
