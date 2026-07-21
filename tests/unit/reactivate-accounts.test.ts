import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Account } from "../../src/lib/instantly-client";
import type {
  LifecycleView,
  AccountDelivery,
} from "../../src/lib/account-lifecycle-sync";

vi.mock("../../src/lib/instantly-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  listAccounts: vi.fn(),
  getAccountRaw: vi.fn(),
  resumeAccount: vi.fn(),
}));
vi.mock("../../src/lib/account-lifecycle-sync", () => ({
  fetchLifecycleByEmail: vi.fn(),
  fetchLatestDeliveryByAccount: vi.fn(),
}));

import {
  listAccounts,
  getAccountRaw,
  resumeAccount,
} from "../../src/lib/instantly-client";
import {
  fetchLifecycleByEmail,
  fetchLatestDeliveryByAccount,
} from "../../src/lib/account-lifecycle-sync";
import {
  selectReactivationCandidates,
  isResumableAccountDetail,
  reactivateEligibleAccounts,
  REACTIVATE_MIN_DEACTIVATED_MS,
  isReactivateAccountsEnabled,
} from "../../src/lib/reactivate-accounts";

const mockListAccounts = vi.mocked(listAccounts);
const mockGetRaw = vi.mocked(getAccountRaw);
const mockResume = vi.mocked(resumeAccount);
const mockLifecycle = vi.mocked(fetchLifecycleByEmail);
const mockDelivery = vi.mocked(fetchLatestDeliveryByAccount);

const NOW = 1_700_000_000_000;
const DAY = REACTIVATE_MIN_DEACTIVATED_MS;

function acct(email: string, health: number | undefined, status = -2): Account {
  return { email, warmup_status: 0, status, stat_warmup_score: health } as Account;
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

describe("selectReactivationCandidates", () => {
  it("selects a healthy, ≥24h-deactivated account whose status is NOT -1/-3", () => {
    const accounts = [acct("good@x.com", 100, -2)];
    const lifecycle = new Map([["good@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)]]);
    const delivery = new Map([["good@x.com", full(true)]]);
    expect(selectReactivationCandidates(accounts, lifecycle, delivery, NOW)).toEqual(["good@x.com"]);
  });

  it("excludes the two never-resumable reasons (-1 OAuth, -3 550) via the LIST status", () => {
    const accounts = [acct("oauth@x.com", 100, -1), acct("throttle@x.com", 100, -3)];
    const lifecycle = new Map([
      ["oauth@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ["throttle@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
    ]);
    const delivery = new Map([["oauth@x.com", full(true)], ["throttle@x.com", full(true)]]);
    expect(selectReactivationCandidates(accounts, lifecycle, delivery, NOW)).toEqual([]);
  });

  it("excludes wrong lifecycle, low health, low inbox, too-recent, and no-age", () => {
    const accounts = [
      acct("inprod@x.com", 100, -2),
      acct("lowhealth@x.com", 99, -2),
      acct("lowinbox@x.com", 100, -2),
      acct("recent@x.com", 100, -2),
      acct("noage@x.com", 100, -2),
    ];
    const lifecycle = new Map([
      ["inprod@x.com", lc("in_production", NOW - DAY - 1)],
      ["lowhealth@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ["lowinbox@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ["recent@x.com", lc("deactivated_by_instantly", NOW - 1000)],
      ["noage@x.com", lc("deactivated_by_instantly", null)],
    ]);
    const delivery = new Map([
      ["inprod@x.com", full(true)],
      ["lowhealth@x.com", full(true)],
      ["lowinbox@x.com", full(false)],
      ["recent@x.com", full(true)],
      ["noage@x.com", full(true)],
    ]);
    expect(selectReactivationCandidates(accounts, lifecycle, delivery, NOW)).toEqual([]);
  });
});

describe("isResumableAccountDetail", () => {
  it("false for a Gmail 550 throttle (responseCode 550)", () => {
    expect(
      isResumableAccountDetail({
        status: -3,
        autofix_failed: true,
        status_message: { responseCode: 550, response: "550-5.4.5 Daily user sending limit exceeded" },
      }),
    ).toBe(false);
  });

  it("false for a 550 detected in the response text even without the code", () => {
    expect(
      isResumableAccountDetail({
        status: -2,
        status_message: { response: "error 5.4.5 daily user sending limit" },
      }),
    ).toBe(false);
  });

  it("false for an OAuth -1 (status_message null) and for autofix_failed", () => {
    expect(isResumableAccountDetail({ status: -1, status_message: null })).toBe(false);
    expect(isResumableAccountDetail({ status: -2, autofix_failed: true })).toBe(false);
  });

  it("true for a plain pause: clean status, no throttle, autofix not failed", () => {
    expect(isResumableAccountDetail({ status: -2, autofix_failed: false, status_message: null })).toBe(true);
    expect(isResumableAccountDetail({ status: 0 })).toBe(true);
  });
});

describe("reactivateEligibleAccounts", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockResume.mockResolvedValue({} as Account);
  });

  it("resumes only candidates whose per-account detail is genuinely resumable", async () => {
    mockListAccounts.mockResolvedValue([acct("pause@x.com", 100, -2), acct("hidden550@x.com", 100, -2)]);
    mockLifecycle.mockResolvedValue(
      new Map([
        ["pause@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
        ["hidden550@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ]),
    );
    mockDelivery.mockResolvedValue(new Map([["pause@x.com", full(true)], ["hidden550@x.com", full(true)]]));
    mockGetRaw.mockImplementation(async (_k, email) =>
      email === "pause@x.com"
        ? { status: -2, autofix_failed: false, status_message: null }
        : { status: -2, autofix_failed: true, status_message: { responseCode: 550 } },
    );

    const summary = await reactivateEligibleAccounts("key", NOW);

    expect(mockResume).toHaveBeenCalledTimes(1);
    expect(mockResume).toHaveBeenCalledWith("key", "pause@x.com");
    expect(summary).toEqual({
      accountsRead: 2,
      candidates: 2,
      reactivated: 1,
      skippedNotResumable: 1,
      failed: 0,
    });
  });

  it("resumes nothing when every deactivation is -1/-3 (prod reality)", async () => {
    mockListAccounts.mockResolvedValue([acct("oauth@x.com", 100, -1), acct("throttle@x.com", 100, -3)]);
    mockLifecycle.mockResolvedValue(
      new Map([
        ["oauth@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
        ["throttle@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ]),
    );
    mockDelivery.mockResolvedValue(new Map([["oauth@x.com", full(true)], ["throttle@x.com", full(true)]]));

    const summary = await reactivateEligibleAccounts("key", NOW);

    expect(mockGetRaw).not.toHaveBeenCalled();
    expect(mockResume).not.toHaveBeenCalled();
    expect(summary).toEqual({ accountsRead: 2, candidates: 0, reactivated: 0, skippedNotResumable: 0, failed: 0 });
  });

  it("fails loud per account: a detail/resume error is counted and the sweep continues", async () => {
    mockListAccounts.mockResolvedValue([acct("a@x.com", 100, -2), acct("b@x.com", 100, -2)]);
    mockLifecycle.mockResolvedValue(
      new Map([
        ["a@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
        ["b@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ]),
    );
    mockDelivery.mockResolvedValue(new Map([["a@x.com", full(true)], ["b@x.com", full(true)]]));
    mockGetRaw.mockResolvedValue({ status: -2, autofix_failed: false, status_message: null });
    mockResume.mockRejectedValueOnce(new Error("boom")).mockResolvedValueOnce({} as Account);

    const summary = await reactivateEligibleAccounts("key", NOW);

    expect(summary).toEqual({
      accountsRead: 2,
      candidates: 2,
      reactivated: 1,
      skippedNotResumable: 0,
      failed: 1,
    });
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
