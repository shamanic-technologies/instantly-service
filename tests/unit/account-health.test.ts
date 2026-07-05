import { describe, it, expect } from "vitest";
import { buildAccountHealth, mapProviderCode } from "../../src/lib/account-health";
import type { Account } from "../../src/lib/instantly-client";

function acc(overrides: Partial<Account> & { email: string }): Account {
  return {
    warmup_status: 1,
    status: 1,
    stat_warmup_score: 100,
    daily_limit: 30,
    ...overrides,
  };
}

describe("buildAccountHealth", () => {
  it("maps a healthy account to the locked shape, all scalars typed, placement null", () => {
    const [row] = buildAccountHealth([
      acc({ email: "jane@send-domain.com", stat_warmup_score: 100, daily_limit: 40 }),
    ]);

    expect(row).toEqual({
      email: "jane@send-domain.com",
      domain: "send-domain.com",
      status: "active",
      warmupScore: 100,
      dailyLimit: 40,
      blocked: false,
      blockReason: null,
      inboxPlacement: null,
      sentToday: 0,
      queueSize: 0,
      accountType: null,
    });
  });

  it("blocks a blacklisted domain with reason 'blacklisted-domain' (same gate as send)", () => {
    // distribute.you is in BLOCKED_DOMAINS; account is otherwise active + warmed.
    const [row] = buildAccountHealth([
      acc({ email: "c@distribute.you", status: 1, stat_warmup_score: 100 }),
    ]);
    expect(row.blocked).toBe(true);
    expect(row.blockReason).toBe("blacklisted-domain");
  });

  it("blocks an inactive account (status <= 0) with reason 'inactive'", () => {
    const [row] = buildAccountHealth([acc({ email: "x@good.com", status: 0 })]);
    expect(row.blocked).toBe(true);
    expect(row.blockReason).toBe("inactive");
    expect(row.status).toBe("inactive");
  });

  it("blocks an under-warmed account (score < 100) with reason 'under-warmed'", () => {
    const [row] = buildAccountHealth([
      acc({ email: "y@good.com", status: 1, stat_warmup_score: 42 }),
    ]);
    expect(row.blocked).toBe(true);
    expect(row.blockReason).toBe("under-warmed");
    expect(row.warmupScore).toBe(42);
  });

  it("uses null (not 0) for genuinely-unknown warmupScore / dailyLimit", () => {
    const [row] = buildAccountHealth([
      { email: "z@good.com", warmup_status: 1, status: 1 } as Account,
    ]);
    expect(row.warmupScore).toBeNull();
    expect(row.dailyLimit).toBeNull();
    // A missing score fails the warmup gate → under-warmed (0 < 100), not silent pass.
    expect(row.blocked).toBe(true);
    expect(row.blockReason).toBe("under-warmed");
  });

  it("precedence mirrors the send gate: inactive wins over domain/warmup", () => {
    const [row] = buildAccountHealth([
      acc({ email: "c@distribute.you", status: 0, stat_warmup_score: 10 }),
    ]);
    expect(row.blockReason).toBe("inactive");
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

  it("reports blockReason 'manual' when the account is in the manually-blacklisted set", () => {
    const [row] = buildAccountHealth(
      [acc({ email: "rested@good.com", status: 1, stat_warmup_score: 100 })],
      new Map(),
      new Map(),
      new Map(),
      new Set(["rested@good.com"]),
    );
    expect(row.blocked).toBe(true);
    expect(row.blockReason).toBe("manual");
  });

  it("'manual' outranks under-warmed / inactive / blacklisted-domain", () => {
    const rows = buildAccountHealth(
      [
        acc({ email: "u@good.com", status: 1, stat_warmup_score: 10 }), // under-warmed
        acc({ email: "i@good.com", status: 0 }), // inactive
        acc({ email: "d@distribute.you", status: 1, stat_warmup_score: 100 }), // blocked domain
      ],
      new Map(),
      new Map(),
      new Map(),
      new Set(["u@good.com", "i@good.com", "d@distribute.you"]),
    );
    expect(rows.every((r) => r.blockReason === "manual")).toBe(true);
  });

  it("accounts NOT in the manual set keep their derived blockReason", () => {
    const [row] = buildAccountHealth(
      [acc({ email: "y@good.com", status: 1, stat_warmup_score: 42 })],
      new Map(),
      new Map(),
      new Map(),
      new Set(["someone-else@good.com"]),
    );
    expect(row.blockReason).toBe("under-warmed");
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
