import { describe, it, expect } from "vitest";
import { selectAccountsNeedingDailyLimit } from "../../src/lib/sync-daily-limit";
import type { Account } from "../../src/lib/instantly-client";

function acct(email: string, daily_limit: number | undefined): Account {
  return { email, warmup_status: 0, status: 1, daily_limit } as Account;
}

describe("selectAccountsNeedingDailyLimit", () => {
  it("returns only accounts whose daily_limit differs from the target", () => {
    const accounts = [
      acct("a@x.com", 40), // needs patch (old cap)
      acct("b@x.com", 50), // aligned → skip
      acct("c@x.com", undefined), // absent → needs patch
      acct("d@x.com", 60), // higher → still differs → patch to target
    ];
    expect(selectAccountsNeedingDailyLimit(accounts, 50)).toEqual([
      "a@x.com",
      "c@x.com",
      "d@x.com",
    ]);
  });

  it("returns empty when every account is already at the target (idempotent no-op)", () => {
    const accounts = [acct("a@x.com", 50), acct("b@x.com", 50)];
    expect(selectAccountsNeedingDailyLimit(accounts, 50)).toEqual([]);
  });

  it("preserves input order", () => {
    const accounts = [acct("z@x.com", 40), acct("a@x.com", 40)];
    expect(selectAccountsNeedingDailyLimit(accounts, 50)).toEqual([
      "z@x.com",
      "a@x.com",
    ]);
  });
});
