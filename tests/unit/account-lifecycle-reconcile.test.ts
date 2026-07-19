import { describe, it, expect, vi, beforeEach } from "vitest";

// ── Mock the DB connection ───────────────────────────────────────────────────
const mockExecute = vi.fn();
const mockInsertValues = vi.fn(async () => undefined);
const mockUpdateWhere = vi.fn(async () => undefined);
const mockUpdateSet = vi.fn(() => ({ where: mockUpdateWhere }));

vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockExecute(...a),
    insert: () => ({ values: (...a: unknown[]) => mockInsertValues(...a) }),
    update: () => ({ set: (...a: unknown[]) => mockUpdateSet(...a) }),
  },
}));

// ── Mock the Instantly warmup + daily-limit PATCHes ──────────────────────────
const mockSetWarmup = vi.fn(async () => ({}));
const mockSetDaily = vi.fn(async () => ({}));
vi.mock("../../src/lib/instantly-client", () => ({
  setWarmupDailyLimit: (...a: unknown[]) => mockSetWarmup(...a),
  setDailyLimit: (...a: unknown[]) => mockSetDaily(...a),
  listAccounts: vi.fn(async () => []),
}));

import { reconcileLifecycle } from "../../src/lib/account-lifecycle-sync";

/**
 * reconcileLifecycle issues exactly three db.execute reads, in order:
 *   1. silver accounts, 2. domain_policy, 3. latest placement delivery.
 * Seed them via mockResolvedValueOnce in that order.
 */
function seedReads(opts: {
  accounts: Array<Record<string, unknown>>;
  domains?: Array<{ domain: string }>;
  delivery?: Array<{ accountEmail: string; inboxCount: number; seedTotal: number }>;
}) {
  mockExecute.mockReset();
  mockExecute
    .mockResolvedValueOnce({ rows: opts.accounts })
    .mockResolvedValueOnce({ rows: opts.domains ?? [] })
    .mockResolvedValueOnce({ rows: opts.delivery ?? [] });
}

beforeEach(() => {
  mockInsertValues.mockClear();
  mockUpdateSet.mockClear();
  mockUpdateWhere.mockClear();
  mockSetWarmup.mockClear();
  mockSetDaily.mockClear();
});

describe("reconcileLifecycle", () => {
  it("promotes to in_production on a real change: PATCHes warmup 5, writes event + silver", async () => {
    seedReads({
      accounts: [
        {
          email: "prod@dfy.com",
          instantlyStatus: 1,
          warmupScore: 100,
          dailyLimit: 30,
          lifecycleStatus: "in_recovery", // current
        },
      ],
      delivery: [{ accountEmail: "prod@dfy.com", inboxCount: 98, seedTotal: 98 }], // 100% → promote
    });

    const summary = await reconcileLifecycle("api-key");

    expect(summary).toEqual({ scanned: 1, changed: 1, warmupPatched: 1, dailyLimitPatched: 1, failed: 0 });
    expect(mockSetWarmup).toHaveBeenCalledWith("api-key", "prod@dfy.com", 5);
    // in_production also opens the campaign daily max-send to 45.
    expect(mockSetDaily).toHaveBeenCalledWith("api-key", "prod@dfy.com", 45);
    expect(mockInsertValues).toHaveBeenCalledTimes(1);
    const event = mockInsertValues.mock.calls[0][0] as Record<string, unknown>;
    expect(event.fromStatus).toBe("in_recovery");
    expect(event.toStatus).toBe("in_production");
    expect(event.reason).toBe("passed");
    expect(mockUpdateSet).toHaveBeenCalledTimes(1);
  });

  it("is idempotent: no change → no event, no warmup PATCH", async () => {
    seedReads({
      accounts: [
        {
          email: "prod@dfy.com",
          instantlyStatus: 1,
          warmupScore: 100,
          dailyLimit: 30,
          lifecycleStatus: "in_production", // already correct
        },
      ],
      delivery: [{ accountEmail: "prod@dfy.com", inboxCount: 98, seedTotal: 98 }],
    });

    const summary = await reconcileLifecycle("api-key");

    expect(summary).toEqual({ scanned: 1, changed: 0, warmupPatched: 0, dailyLimitPatched: 0, failed: 0 });
    expect(mockSetWarmup).not.toHaveBeenCalled();
    expect(mockInsertValues).not.toHaveBeenCalled();
    expect(mockUpdateSet).not.toHaveBeenCalled();
  });

  it("untested account (no delivery) → in_recovery, warmup 30 + daily 20", async () => {
    seedReads({
      accounts: [
        {
          email: "new@dfy.com",
          instantlyStatus: 1,
          warmupScore: 100,
          dailyLimit: 30,
          lifecycleStatus: null, // never classified
        },
      ],
      delivery: [], // never tested
    });

    const summary = await reconcileLifecycle("api-key");

    expect(summary.changed).toBe(1);
    expect(mockSetWarmup).toHaveBeenCalledWith("api-key", "new@dfy.com", 30);
    // in_recovery now also caps the campaign daily max-send to 20.
    expect(mockSetDaily).toHaveBeenCalledWith("api-key", "new@dfy.com", 20);
    const event = mockInsertValues.mock.calls[0][0] as Record<string, unknown>;
    expect(event.fromStatus).toBeNull();
    expect(event.toStatus).toBe("in_recovery");
  });

  it("brand domain → deactivated_by_user, warmup 30", async () => {
    seedReads({
      accounts: [
        {
          email: "cold@distribute.you",
          instantlyStatus: 1,
          warmupScore: 100,
          dailyLimit: 30,
          lifecycleStatus: null,
        },
      ],
      domains: [{ domain: "distribute.you" }],
      delivery: [{ accountEmail: "cold@distribute.you", inboxCount: 98, seedTotal: 98 }],
    });

    const summary = await reconcileLifecycle("api-key");

    expect(summary.changed).toBe(1);
    expect(mockSetWarmup).toHaveBeenCalledWith("api-key", "cold@distribute.you", 30);
    // deactivated_by_user leaves the campaign daily_limit untouched (queue drains).
    expect(mockSetDaily).not.toHaveBeenCalled();
    const event = mockInsertValues.mock.calls[0][0] as Record<string, unknown>;
    expect(event.toStatus).toBe("deactivated_by_user");
    expect(event.reason).toBe("brand_domain");
  });

  it("deactivated_by_instantly → no warmup PATCH (account is off)", async () => {
    seedReads({
      accounts: [
        {
          email: "off@dfy.com",
          instantlyStatus: 0, // Instantly disabled it
          warmupScore: 100,
          dailyLimit: 30,
          lifecycleStatus: "in_production",
        },
      ],
      delivery: [{ accountEmail: "off@dfy.com", inboxCount: 98, seedTotal: 98 }],
    });

    const summary = await reconcileLifecycle("api-key");

    expect(summary).toEqual({ scanned: 1, changed: 1, warmupPatched: 0, dailyLimitPatched: 0, failed: 0 });
    expect(mockSetDaily).not.toHaveBeenCalled();
    expect(mockSetWarmup).not.toHaveBeenCalled();
    const event = mockInsertValues.mock.calls[0][0] as Record<string, unknown>;
    expect(event.toStatus).toBe("deactivated_by_instantly");
  });

  it("reactivation: leaving deactivated_by_instantly reports reason 'reactivated'", async () => {
    seedReads({
      accounts: [
        {
          email: "back@dfy.com",
          instantlyStatus: 1, // re-enabled
          warmupScore: 100,
          dailyLimit: 30,
          lifecycleStatus: "deactivated_by_instantly", // was off
        },
      ],
      delivery: [{ accountEmail: "back@dfy.com", inboxCount: 98, seedTotal: 98 }],
    });

    await reconcileLifecycle("api-key");

    const event = mockInsertValues.mock.calls[0][0] as Record<string, unknown>;
    expect(event.toStatus).toBe("in_production");
    expect(event.reason).toBe("reactivated");
    expect(mockSetWarmup).toHaveBeenCalledWith("api-key", "back@dfy.com", 5);
  });

  it("warmup PATCH failure → counted failed, no event/silver persisted (no half-applied state)", async () => {
    seedReads({
      accounts: [
        {
          email: "flaky@dfy.com",
          instantlyStatus: 1,
          warmupScore: 100,
          dailyLimit: 30,
          lifecycleStatus: "in_recovery",
        },
      ],
      delivery: [{ accountEmail: "flaky@dfy.com", inboxCount: 98, seedTotal: 98 }],
    });
    mockSetWarmup.mockRejectedValueOnce(new Error("Instantly 500"));

    const summary = await reconcileLifecycle("api-key");

    expect(summary).toEqual({ scanned: 1, changed: 0, warmupPatched: 0, dailyLimitPatched: 0, failed: 1 });
    expect(mockInsertValues).not.toHaveBeenCalled();
    expect(mockUpdateSet).not.toHaveBeenCalled();
  });
});
