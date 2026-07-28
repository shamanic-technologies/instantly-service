import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Account } from "../../src/lib/instantly-client";
import type { LifecycleView } from "../../src/lib/account-lifecycle-sync";

vi.mock("../../src/lib/instantly-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  listAccounts: vi.fn(),
  getAccountRaw: vi.fn(),
  resumeAccount: vi.fn(),
}));
vi.mock("../../src/lib/account-lifecycle-sync", () => ({
  fetchLifecycleByEmail: vi.fn(),
}));

import {
  listAccounts,
  getAccountRaw,
  resumeAccount,
} from "../../src/lib/instantly-client";
import { fetchLifecycleByEmail } from "../../src/lib/account-lifecycle-sync";
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

describe("selectReactivationCandidates", () => {
  it("selects a ≥24h-deactivated account regardless of its Instantly status code", () => {
    const accounts = [acct("good@x.com", 100, -2)];
    const lifecycle = new Map([["good@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)]]);
    expect(selectReactivationCandidates(accounts, lifecycle, NOW)).toEqual(["good@x.com"]);
  });

  // Prod audit 2026-07-28: the blunt `status in {-1,-3}` exclusion blocked 12
  // mailboxes whose real SMTP failure was a TRANSIENT 450. The status code is no
  // longer a gate — the SMTP reply class is (isResumableAccountDetail).
  it("no longer excludes -1 (OAuth) or -3 on the status code alone", () => {
    const accounts = [acct("oauth@x.com", 100, -1), acct("dash3@x.com", 100, -3)];
    const lifecycle = new Map([
      ["oauth@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ["dash3@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
    ]);
    expect(selectReactivationCandidates(accounts, lifecycle, NOW)).toEqual([
      "oauth@x.com",
      "dash3@x.com",
    ]);
  });

  // Health + delivery are NOT gates here: resume only sets Instantly status 1,
  // and deriveLifecycle lands a below-bar account in `in_recovery` (zero new
  // sends). Gating on delivery deadlocked the account out of ever being tested.
  it("selects a low-health, never-placement-tested account (no health/delivery gate)", () => {
    const accounts = [acct("lowhealth@x.com", 42, -3)];
    const lifecycle = new Map([["lowhealth@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)]]);
    expect(selectReactivationCandidates(accounts, lifecycle, NOW)).toEqual(["lowhealth@x.com"]);
  });

  it("excludes wrong lifecycle, too-recent, and no-age", () => {
    const accounts = [
      acct("inprod@x.com", 100, -2),
      acct("recovery@x.com", 100, -2),
      acct("recent@x.com", 100, -2),
      acct("noage@x.com", 100, -2),
    ];
    const lifecycle = new Map([
      ["inprod@x.com", lc("in_production", NOW - DAY - 1)],
      ["recovery@x.com", lc("in_recovery", NOW - DAY - 1)],
      ["recent@x.com", lc("deactivated_by_instantly", NOW - 1000)],
      ["noage@x.com", lc("deactivated_by_instantly", null)],
    ]);
    expect(selectReactivationCandidates(accounts, lifecycle, NOW)).toEqual([]);
  });

  it("excludes an account absent from the live Instantly list", () => {
    const lifecycle = new Map([["gone@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)]]);
    expect(selectReactivationCandidates([], lifecycle, NOW)).toEqual([]);
  });
});

describe("isResumableAccountDetail", () => {
  it("false for a real Gmail 550 throttle (responseCode 550)", () => {
    expect(
      isResumableAccountDetail({
        status: -3,
        autofix_failed: true,
        status_message: { responseCode: 550, response: "550-5.4.5 Daily user sending limit exceeded" },
      }),
    ).toBe(false);
  });

  it("false for a 5xx detected in the response text even without the code", () => {
    expect(
      isResumableAccountDetail({
        status: -2,
        status_message: { response: "error 5.4.5 daily user sending limit" },
      }),
    ).toBe(false);
    expect(
      isResumableAccountDetail({ status: -2, status_message: { response: "550 mailbox unavailable" } }),
    ).toBe(false);
  });

  it("false when only the lowercased e_message carries the 5xx", () => {
    expect(
      isResumableAccountDetail({
        status: -3,
        status_message: { e_message: "error: can't send mail - 5.7.1 message blocked" },
      }),
    ).toBe(false);
  });

  // The 9/12 prod case: Instantly deactivated OUR sender because the PROSPECT's
  // domain does not resolve. Transient, and not even our fault.
  it("true for a transient 450 4.1.2 dead-recipient-domain rejection", () => {
    expect(
      isResumableAccountDetail({
        status: -3,
        autofix_failed: true,
        status_message: {
          code: "EENVELOPE",
          responseCode: 450,
          response: "450 4.1.2 <drmccomb@kingwoodchiro.com>: Recipient address rejected: Domain not found",
          e_message:
            "error: can't send mail - all recipients were rejected: 450 4.1.2 <drmccomb@kingwoodchiro.com>: recipient address rejected: domain not found",
        },
      }),
    ).toBe(true);
  });

  // The other 2/12: the Gandi relay's own per-user cap — a 450, NOT a Gmail 550.
  it("true for a transient 450 4.7.1 relay per-user cap", () => {
    expect(
      isResumableAccountDetail({
        status: -3,
        autofix_failed: true,
        status_message: {
          responseCode: 450,
          response:
            "450 4.7.1 <chad@insideoutlab.com>: Recipient address rejected: Too many mail per day for sasl kevin@growthagency.diy",
        },
      }),
    ).toBe(true);
  });

  it("true for an OAuth -1 (no recorded SMTP failure) and for autofix_failed alone", () => {
    expect(isResumableAccountDetail({ status: -1, status_message: null, autofix_failed: true })).toBe(true);
    expect(isResumableAccountDetail({ status: -2, autofix_failed: true })).toBe(true);
  });

  it("true for a plain pause with no status_message", () => {
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

  // The 2026-07-28 prod fleet: 12 `-3` accounts, every one a transient 450. The
  // old gate resumed ZERO of them; the SMTP-class gate resumes all 12.
  it("resumes a -3 account whose recorded failure is a transient 450", async () => {
    mockListAccounts.mockResolvedValue([acct("relay@x.com", 89, -3)]);
    mockLifecycle.mockResolvedValue(
      new Map([["relay@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)]]),
    );
    mockGetRaw.mockResolvedValue({
      status: -3,
      autofix_failed: true,
      status_message: {
        responseCode: 450,
        response: "450 4.1.2 <dead@gone.com>: Recipient address rejected: Domain not found",
      },
    });

    const summary = await reactivateEligibleAccounts("key", NOW);

    expect(mockResume).toHaveBeenCalledWith("key", "relay@x.com");
    expect(summary).toEqual({
      accountsRead: 1,
      candidates: 1,
      reactivated: 1,
      skippedNotResumable: 0,
      failed: 0,
    });
  });

  it("skips a genuine Gmail 550 throttle after paying the detail fetch", async () => {
    mockListAccounts.mockResolvedValue([acct("gmail@x.com", 100, -3)]);
    mockLifecycle.mockResolvedValue(
      new Map([["gmail@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)]]),
    );
    mockGetRaw.mockResolvedValue({
      status: -3,
      status_message: { responseCode: 550, response: "550-5.4.5 Daily user sending limit exceeded" },
    });

    const summary = await reactivateEligibleAccounts("key", NOW);

    expect(mockResume).not.toHaveBeenCalled();
    expect(summary).toEqual({
      accountsRead: 1,
      candidates: 1,
      reactivated: 0,
      skippedNotResumable: 1,
      failed: 0,
    });
  });

  it("fails loud per account: a detail/resume error is counted and the sweep continues", async () => {
    mockListAccounts.mockResolvedValue([acct("a@x.com", 100, -2), acct("b@x.com", 100, -2)]);
    mockLifecycle.mockResolvedValue(
      new Map([
        ["a@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
        ["b@x.com", lc("deactivated_by_instantly", NOW - DAY - 1)],
      ]),
    );
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
