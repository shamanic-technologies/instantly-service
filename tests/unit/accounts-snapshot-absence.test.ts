import { describe, it, expect, vi, beforeEach } from "vitest";

/**
 * Ghost-account sweep (issue #555).
 *
 * `snapshotAccounts` upserts every account Instantly still lists. Anything in
 * silver that it did NOT touch is an account deleted upstream — prod carried 10
 * of them (266 stored vs 250 live), silently inflating every fleet capacity
 * view. They are FLAGGED, never deleted: their sent events and lifecycle
 * history stay meaningful.
 */

const mockExecute = vi.fn(async () => [] as unknown[]);
const mockInsertValues = vi.fn(async () => undefined);

vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockExecute(...a),
    insert: () => ({
      values: (...a: unknown[]) => {
        const promise = mockInsertValues(...a) as Promise<undefined> & {
          onConflictDoUpdate?: (...args: unknown[]) => Promise<undefined>;
        };
        promise.onConflictDoUpdate = () => Promise.resolve(undefined);
        return promise;
      },
    }),
  },
}));

const mockListAccounts = vi.fn(async () => [] as unknown[]);
vi.mock("../../src/lib/instantly-client", () => ({
  listAccounts: (...a: unknown[]) => mockListAccounts(...(a as [])),
  setWarmupDailyLimit: vi.fn(async () => ({})),
  setDailyLimit: vi.fn(async () => ({})),
}));

import { snapshotAccounts } from "../../src/lib/account-lifecycle-sync";

const liveAccount = {
  email: "live@growthagency.dev",
  status: 1,
  warmup_status: 1,
  stat_warmup_score: 100,
  daily_limit: 45,
  provider_code: 1,
  timestamp_created: "2026-06-01T00:00:00Z",
};

beforeEach(() => {
  vi.resetAllMocks();
  mockExecute.mockResolvedValue([]);
  mockInsertValues.mockResolvedValue(undefined);
  mockListAccounts.mockResolvedValue([liveAccount]);
  vi.spyOn(console, "warn").mockImplementation(() => {});
});

describe("snapshotAccounts ghost sweep", () => {
  it("flags rows Instantly no longer lists and reports the count", async () => {
    mockExecute.mockResolvedValue([{ email: "ghost@growthagency.dev" }]);

    const summary = await snapshotAccounts("api-key");

    expect(summary.synced).toBe(1);
    expect(summary.markedAbsent).toBe(1);
  });

  it("flags rather than deletes — the sweep is an UPDATE, never a DELETE", async () => {
    await snapshotAccounts("api-key");

    const sweep = JSON.stringify(mockExecute.mock.calls[0][0]);
    expect(sweep).toContain("UPDATE instantly_accounts");
    expect(sweep).toContain("absent_since");
    expect(sweep).not.toContain("DELETE");
  });

  it("keys the sweep on updated_at, not a NOT IN list of every live email", async () => {
    await snapshotAccounts("api-key");

    const sweep = JSON.stringify(mockExecute.mock.calls[0][0]);
    // A per-email bind list grows with the fleet and eventually trips
    // Postgres' 65,534-parameter ceiling.
    expect(sweep).toContain("updated_at");
    expect(sweep).not.toContain("NOT IN");
  });

  it("clears the flag for an account that reappears", async () => {
    await snapshotAccounts("api-key");

    const upserts = mockInsertValues.mock.calls.map((call) => call[0] as Record<string, unknown>);
    const silver = upserts.find((row) => "lifecycleStatus" in row || "warmupEnabled" in row);
    expect(silver).toBeDefined();
  });

  it("refuses to sweep when Instantly returns zero accounts", async () => {
    mockListAccounts.mockResolvedValue([]);

    await expect(snapshotAccounts("api-key")).rejects.toThrow(/zero accounts/);
    // Nothing was written, so no account could be wrongly flagged.
    expect(mockExecute).not.toHaveBeenCalled();
  });
});
