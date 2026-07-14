import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Account } from "../../src/lib/instantly-client";

vi.mock("../../src/lib/instantly-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  listAccounts: vi.fn(),
  setSlowRamp: vi.fn(),
}));

import { listAccounts, setSlowRamp } from "../../src/lib/instantly-client";
import {
  selectAccountsNeedingSlowRampOff,
  syncSlowRampOff,
} from "../../src/lib/sync-slow-ramp";

const mockListAccounts = vi.mocked(listAccounts);
const mockSetSlowRamp = vi.mocked(setSlowRamp);

function acct(email: string, enable_slow_ramp: boolean | undefined): Account {
  return { email, warmup_status: 0, status: 1, enable_slow_ramp } as Account;
}

describe("selectAccountsNeedingSlowRampOff", () => {
  it("selects accounts whose enable_slow_ramp is true OR absent, skips already-false", () => {
    const accounts = [
      acct("a@x.com", true), // ramp on → needs off
      acct("b@x.com", false), // already off → skip
      acct("c@x.com", undefined), // absent → fail-safe → needs off
    ];
    expect(selectAccountsNeedingSlowRampOff(accounts)).toEqual([
      "a@x.com",
      "c@x.com",
    ]);
  });

  it("returns empty when every account is already off (idempotent no-op)", () => {
    const accounts = [acct("a@x.com", false), acct("b@x.com", false)];
    expect(selectAccountsNeedingSlowRampOff(accounts)).toEqual([]);
  });

  it("preserves input order and filters empty emails", () => {
    const accounts = [acct("z@x.com", true), acct("", true), acct("a@x.com", true)];
    expect(selectAccountsNeedingSlowRampOff(accounts)).toEqual([
      "z@x.com",
      "a@x.com",
    ]);
  });
});

describe("syncSlowRampOff", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSetSlowRamp.mockResolvedValue({} as Account);
  });

  it("PATCHes only the accounts needing off, counts patched/skipped", async () => {
    mockListAccounts.mockResolvedValue([
      acct("a@x.com", true),
      acct("b@x.com", false),
      acct("c@x.com", undefined),
    ]);

    const summary = await syncSlowRampOff("key");

    expect(mockSetSlowRamp).toHaveBeenCalledTimes(2);
    expect(mockSetSlowRamp).toHaveBeenCalledWith("key", "a@x.com", false);
    expect(mockSetSlowRamp).toHaveBeenCalledWith("key", "c@x.com", false);
    expect(summary).toEqual({
      accountsRead: 3,
      skipped: 1,
      patched: 2,
      failed: 0,
    });
  });

  it("bounds the batch by limit", async () => {
    mockListAccounts.mockResolvedValue([
      acct("a@x.com", true),
      acct("b@x.com", true),
      acct("c@x.com", true),
    ]);

    const summary = await syncSlowRampOff("key", 2);

    expect(mockSetSlowRamp).toHaveBeenCalledTimes(2);
    expect(summary.patched).toBe(2);
    expect(summary.accountsRead).toBe(3);
  });

  it("fails loud per account: a PATCH error is counted and the sweep continues", async () => {
    mockListAccounts.mockResolvedValue([
      acct("a@x.com", true),
      acct("b@x.com", true),
    ]);
    mockSetSlowRamp
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValueOnce({} as Account);

    const summary = await syncSlowRampOff("key");

    expect(mockSetSlowRamp).toHaveBeenCalledTimes(2);
    expect(summary).toEqual({
      accountsRead: 2,
      skipped: 0,
      patched: 1,
      failed: 1,
    });
  });
});
