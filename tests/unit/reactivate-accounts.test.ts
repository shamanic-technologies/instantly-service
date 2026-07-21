import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Account } from "../../src/lib/instantly-client";
import type {
  LifecycleView,
  AccountDelivery,
} from "../../src/lib/account-lifecycle-sync";

vi.mock("../../src/lib/instantly-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  listAccounts: vi.fn(),
  resumeAccount: vi.fn(),
}));
vi.mock("../../src/lib/account-lifecycle-sync", () => ({
  fetchLifecycleByEmail: vi.fn(),
  fetchLatestDeliveryByAccount: vi.fn(),
}));

import { listAccounts, resumeAccount } from "../../src/lib/instantly-client";
import {
  fetchLifecycleByEmail,
  fetchLatestDeliveryByAccount,
} from "../../src/lib/account-lifecycle-sync";
import {
  selectReactivatable,
  reactivateEligibleAccounts,
  REACTIVATE_MIN_DEACTIVATED_MS,
  isReactivateAccountsEnabled,
} from "../../src/lib/reactivate-accounts";

const mockListAccounts = vi.mocked(listAccounts);
const mockResume = vi.mocked(resumeAccount);
const mockLifecycle = vi.mocked(fetchLifecycleByEmail);
const mockDelivery = vi.mocked(fetchLatestDeliveryByAccount);

const NOW = 1_700_000_000_000; // fixed reference
const DAY = REACTIVATE_MIN_DEACTIVATED_MS;

function acct(email: string, health: number | undefined): Account {
  return { email, warmup_status: 0, status: -1, stat_warmup_score: health } as Account;
}
function lc(status: string, updatedAtMs: number | null): LifecycleView {
  return {
    status: status as LifecycleView["status"],
    reason: null,
    updatedAt: updatedAtMs === null ? null : new Date(updatedAtMs).toISOString(),
  };
}
function full(isFull: boolean): AccountDelivery {
  return { inboxCount: isFull ? 10 : 4, seedTotal: 10, deliveryPct: isFull ? 100 : 40, full: isFull };
}

describe("selectReactivatable", () => {
  it("selects a deactivated_by_instantly account at 100 health, 100% inbox, deactivated ≥24h", () => {
    const accounts = [acct("good@x.com", 100)];
    const lifecycle = new Map([["good@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)]]);
    const delivery = new Map([["good@x.com", full(true)]]);
    expect(selectReactivatable(accounts, lifecycle, delivery, NOW)).toEqual(["good@x.com"]);
  });

  it("excludes accounts that fail any gate", () => {
    const accounts = [
      acct("inprod@x.com", 100), // wrong lifecycle
      acct("lowhealth@x.com", 99), // health < 100
      acct("lowinbox@x.com", 100), // delivery not full
      acct("tooRecent@x.com", 100), // deactivated < 24h ago (backoff)
      acct("noage@x.com", 100), // no updatedAt
    ];
    const lifecycle = new Map([
      ["inprod@x.com", lc("in_production", NOW - DAY - 1)],
      ["lowhealth@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ["lowinbox@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ["tooRecent@x.com", lc("deactivated_by_instantly", NOW - 1000)],
      ["noage@x.com", lc("deactivated_by_instantly", null)],
    ]);
    const delivery = new Map([
      ["inprod@x.com", full(true)],
      ["lowhealth@x.com", full(true)],
      ["lowinbox@x.com", full(false)],
      ["tooRecent@x.com", full(true)],
      ["noage@x.com", full(true)],
    ]);
    expect(selectReactivatable(accounts, lifecycle, delivery, NOW)).toEqual([]);
  });

  it("excludes an account never placement-tested (no delivery row)", () => {
    const accounts = [acct("untested@x.com", 100)];
    const lifecycle = new Map([["untested@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)]]);
    const delivery = new Map<string, AccountDelivery>(); // absent
    expect(selectReactivatable(accounts, lifecycle, delivery, NOW)).toEqual([]);
  });
});

describe("reactivateEligibleAccounts", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockResume.mockResolvedValue({} as Account);
  });

  it("resumes the eligible accounts and reports the summary", async () => {
    mockListAccounts.mockResolvedValue([acct("a@x.com", 100), acct("b@x.com", 100)]);
    mockLifecycle.mockResolvedValue(
      new Map([
        ["a@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
        ["b@x.com", lc("deactivated_by_instantly", NOW - 1000)], // too recent
      ]),
    );
    mockDelivery.mockResolvedValue(new Map([["a@x.com", full(true)], ["b@x.com", full(true)]]));

    const summary = await reactivateEligibleAccounts("key", NOW);

    expect(mockResume).toHaveBeenCalledTimes(1);
    expect(mockResume).toHaveBeenCalledWith("key", "a@x.com");
    expect(summary).toEqual({ accountsRead: 2, eligible: 1, reactivated: 1, failed: 0 });
  });

  it("fails loud per account: a resume error is counted and the sweep continues", async () => {
    mockListAccounts.mockResolvedValue([acct("a@x.com", 100), acct("b@x.com", 100)]);
    mockLifecycle.mockResolvedValue(
      new Map([
        ["a@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
        ["b@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ]),
    );
    mockDelivery.mockResolvedValue(new Map([["a@x.com", full(true)], ["b@x.com", full(true)]]));
    mockResume.mockRejectedValueOnce(new Error("boom")).mockResolvedValueOnce({} as Account);

    const summary = await reactivateEligibleAccounts("key", NOW);

    expect(summary).toEqual({ accountsRead: 2, eligible: 2, reactivated: 1, failed: 1 });
  });
});

describe("isReactivateAccountsEnabled", () => {
  it("is OFF unless REACTIVATE_ACCOUNTS_ENABLED === 'true'", () => {
    const prev = process.env.REACTIVATE_ACCOUNTS_ENABLED;
    delete process.env.REACTIVATE_ACCOUNTS_ENABLED;
    expect(isReactivateAccountsEnabled()).toBe(false);
    process.env.REACTIVATE_ACCOUNTS_ENABLED = "1";
    expect(isReactivateAccountsEnabled()).toBe(false);
    process.env.REACTIVATE_ACCOUNTS_ENABLED = "true";
    expect(isReactivateAccountsEnabled()).toBe(true);
    if (prev === undefined) delete process.env.REACTIVATE_ACCOUNTS_ENABLED;
    else process.env.REACTIVATE_ACCOUNTS_ENABLED = prev;
  });
});
