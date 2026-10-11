import { describe, it, expect, vi } from "vitest";

vi.mock("../../src/db", () => ({ db: { execute: vi.fn() } }));

import { accountFromSilver } from "../../src/lib/silver-accounts";

describe("accountFromSilver — the account list without a live Instantly read", () => {
  it("keeps the last snapshot payload and lays OUR limit and status over it", () => {
    const account = accountFromSilver({
      email: "amy@saviolabsco.com",
      payload: {
        email: "amy@saviolabsco.com",
        first_name: "Amy",
        daily_limit: 20,
        status: -1,
        warmup_status: 1,
        stat_warmup_score: 97,
        timestamp_created: "2026-08-01T00:00:00Z",
      },
      dailyLimit: 50,
      instantlyStatus: 1,
    });

    expect(account).toMatchObject({
      email: "amy@saviolabsco.com",
      first_name: "Amy",
      daily_limit: 50,
      status: 1,
      warmup_status: 1,
      stat_warmup_score: 97,
    });
  });

  it("an account never snapshotted still appears, with only what silver knows", () => {
    const account = accountFromSilver({
      email: "new@saviolabsco.com",
      payload: null,
      dailyLimit: null,
      instantlyStatus: null,
    });

    expect(account).toEqual({ email: "new@saviolabsco.com", warmup_status: 0, status: 0 });
  });
});
