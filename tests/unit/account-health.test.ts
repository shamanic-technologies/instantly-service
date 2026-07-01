import { describe, it, expect } from "vitest";
import { buildAccountHealth } from "../../src/lib/account-health";
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
});
