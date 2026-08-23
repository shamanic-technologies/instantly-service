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

import {
  reconcileLifecycle,
  fetchInProductionAccounts,
} from "../../src/lib/account-lifecycle-sync";

/**
 * reconcileLifecycle issues exactly three db.execute reads, in order:
 *   1. silver accounts, 2. domain_policy, 3. latest placement delivery.
 * Seed them via mockResolvedValueOnce in that order.
 */
function seedReads(opts: {
  accounts: Array<Record<string, unknown>>;
  domains?: Array<{ domain: string }>;
  delivery?: Array<{
    accountEmail: string;
    inboxCount: number;
    seedTotal: number;
    testedAt?: string;
  }>;
}) {
  // Placement rows carry a `tested_at`; without one the evidence reads as stale
  // and nothing can promote. Default every fixture to "tested just now" so only
  // the tests that care about freshness have to say so.
  const now = new Date().toISOString();
  const delivery = (opts.delivery ?? []).map((d) => ({ testedAt: now, ...d }));
  mockExecute.mockReset();
  mockExecute
    .mockResolvedValueOnce({ rows: opts.accounts })
    .mockResolvedValueOnce({ rows: opts.domains ?? [] })
    .mockResolvedValueOnce({ rows: delivery });
}

beforeEach(() => {
  mockInsertValues.mockClear();
  mockUpdateSet.mockClear();
  mockUpdateWhere.mockClear();
  mockSetWarmup.mockClear();
  mockSetDaily.mockClear();
});

describe("fetchInProductionAccounts — infra vendor attribution", () => {
  it("resolves each account's infra vendor from infra_domains and returns it", async () => {
    mockExecute.mockReset();
    mockExecute.mockResolvedValueOnce({
      rows: [
        {
          email: "kevin@pressbeat.ai",
          firstName: "Kevin",
          lastName: "Lourd",
          instantlyStatus: 1,
          warmupScore: 100,
          dailyLimit: 45,
          providerCode: 1,
          timestampCreated: "2026-01-01T00:00:00.000Z",
          infraProvider: "gandi",
        },
        {
          // No infra_domains row for this domain → null, which sorts LAST in
          // accountFillOrder rather than first. Never fabricated.
          email: "orphan@nowhere.com",
          firstName: null,
          lastName: null,
          instantlyStatus: 1,
          warmupScore: 100,
          dailyLimit: 45,
          providerCode: 2,
          timestampCreated: null,
          infraProvider: null,
        },
      ],
    });

    const accounts = await fetchInProductionAccounts(null);

    expect(accounts.map((a) => [a.email, a.infraProvider])).toEqual([
      ["kevin@pressbeat.ai", "gandi"],
      ["orphan@nowhere.com", null],
    ]);

    // The vendor is joined from the inventory table on the email's domain, and
    // a domain reported by two vendors resolves to the one that fills earliest.
    const sqlText = JSON.stringify(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("infra_domains");
    expect(sqlText).toContain("split_part");
    expect(sqlText).toContain("infraProvider");
  });
});

describe("reconcileLifecycle", () => {
  it("promotes to in_production on a real change: PATCHes warmup 0 + daily 50, writes event + silver", async () => {
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

    expect(summary).toEqual({ scanned: 1, changed: 1, warmupPatched: 1, dailyLimitPatched: 1, reasonsRefreshed: 0, failed: 0 });
    expect(mockSetWarmup).toHaveBeenCalledWith("api-key", "prod@dfy.com", 0);
    // in_production also opens the campaign daily max-send to 45.
    expect(mockSetDaily).toHaveBeenCalledWith("api-key", "prod@dfy.com", 50);
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
          lifecycleReason: "passed", // and its reason already matches the derived one
        },
      ],
      delivery: [{ accountEmail: "prod@dfy.com", inboxCount: 98, seedTotal: 98 }],
    });

    const summary = await reconcileLifecycle("api-key");

    expect(summary).toEqual({ scanned: 1, changed: 0, warmupPatched: 0, dailyLimitPatched: 0, reasonsRefreshed: 0, failed: 0 });
    expect(mockSetWarmup).not.toHaveBeenCalled();
    expect(mockInsertValues).not.toHaveBeenCalled();
    expect(mockUpdateSet).not.toHaveBeenCalled();
  });

  it("status unchanged but reason STALE → refreshes lifecycle_reason only (no event, no PATCH, no lifecycleUpdatedAt)", async () => {
    // The account entered in_recovery on `health_below_bar` and has since
    // recovered its health to 100 — it is still held back by delivery, so the
    // STATUS does not flip and the pre-fix code left the reason frozen at
    // `health_below_bar` next to a health of 100 (a self-contradictory ops row).
    seedReads({
      accounts: [
        {
          email: "stale@dfy.com",
          instantlyStatus: 1,
          warmupScore: 100,
          dailyLimit: 20,
          lifecycleStatus: "in_recovery",
          lifecycleReason: "health_below_bar",
        },
      ],
      delivery: [{ accountEmail: "stale@dfy.com", inboxCount: 30, seedTotal: 40 }], // 75% → below bar
    });

    const summary = await reconcileLifecycle("api-key");

    expect(summary).toEqual({
      scanned: 1,
      changed: 0,
      warmupPatched: 0,
      dailyLimitPatched: 0,
      reasonsRefreshed: 1,
      failed: 0,
    });
    // No transition → no Instantly PATCH and no lifecycle event.
    expect(mockSetWarmup).not.toHaveBeenCalled();
    expect(mockSetDaily).not.toHaveBeenCalled();
    expect(mockInsertValues).not.toHaveBeenCalled();
    // Silver reason refreshed; lifecycleStatus / lifecycleUpdatedAt untouched
    // (reactivate-accounts reads lifecycleUpdatedAt as the deactivation-age proxy).
    expect(mockUpdateSet).toHaveBeenCalledTimes(1);
    const patch = mockUpdateSet.mock.calls[0][0] as Record<string, unknown>;
    expect(patch.lifecycleReason).toBe("delivery_below_bar");
    expect(patch).not.toHaveProperty("lifecycleStatus");
    expect(patch).not.toHaveProperty("lifecycleUpdatedAt");
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

    expect(summary).toEqual({ scanned: 1, changed: 1, warmupPatched: 0, dailyLimitPatched: 0, reasonsRefreshed: 0, failed: 0 });
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
    expect(mockSetWarmup).toHaveBeenCalledWith("api-key", "back@dfy.com", 0);
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

    expect(summary).toEqual({ scanned: 1, changed: 0, warmupPatched: 0, dailyLimitPatched: 0, reasonsRefreshed: 0, failed: 1 });
    expect(mockInsertValues).not.toHaveBeenCalled();
    expect(mockUpdateSet).not.toHaveBeenCalled();
  });

  it("smtp: an Instantly-DISABLED account promotes, with ZERO Instantly PATCH", async () => {
    // The end-to-end shape of the transport branch. Instantly disabled this
    // mailbox (-1), which on the instantly transport is a hard stop; on smtp we
    // hold the credential ourselves, so only the delivery measurement gates it.
    //
    // And the PATCHes must NOT fire: reconcile PATCHes BEFORE it persists and
    // skips the persist on error, so a PATCH aimed at a dead Instantly account
    // would leave the flip permanently unable to land (exactly the `failed: 1`
    // case above).
    seedReads({
      accounts: [
        {
          email: "self@fuseconnectio.com",
          instantlyStatus: -1,
          warmupScore: 0, // structurally 0 — it is not in Instantly's warmup pool
          dailyLimit: 20,
          sendTransport: "smtp",
          lifecycleStatus: "deactivated_by_instantly",
        },
      ],
      delivery: [
        { accountEmail: "self@fuseconnectio.com", inboxCount: 37, seedTotal: 39 }, // 94.9% → at bar
      ],
    });

    const summary = await reconcileLifecycle("api-key");

    expect(summary).toEqual({
      scanned: 1,
      changed: 1,
      warmupPatched: 0,
      dailyLimitPatched: 0,
      reasonsRefreshed: 0,
      failed: 0,
    });
    expect(mockSetWarmup).not.toHaveBeenCalled();
    expect(mockSetDaily).not.toHaveBeenCalled();
    const event = mockInsertValues.mock.calls[0][0] as Record<string, unknown>;
    expect(event.fromStatus).toBe("deactivated_by_instantly");
    expect(event.toStatus).toBe("in_production");
    // Leaving deactivated_by_instantly reports `reactivated`, whatever the pipe.
    expect(event.reason).toBe("reactivated");
  });

  it("smtp: a mailbox BELOW the delivery bar still lands in_recovery and sends nothing", async () => {
    // The safety half. Moving a dead mailbox onto our own pipe does not promote
    // it — it only stops excluding it for a reason that no longer applies.
    seedReads({
      accounts: [
        {
          email: "dead@growthagency.studio",
          instantlyStatus: -3,
          warmupScore: 0,
          dailyLimit: 20,
          sendTransport: "smtp",
          lifecycleStatus: "deactivated_by_instantly",
        },
      ],
      delivery: [
        { accountEmail: "dead@growthagency.studio", inboxCount: 0, seedTotal: 29 }, // 0% inbox
      ],
    });

    const summary = await reconcileLifecycle("api-key");

    expect(summary.changed).toBe(1);
    expect(mockSetWarmup).not.toHaveBeenCalled();
    expect(mockSetDaily).not.toHaveBeenCalled();
    const event = mockInsertValues.mock.calls[0][0] as Record<string, unknown>;
    expect(event.toStatus).toBe("in_recovery");
  });

  it("an unrecognised send_transport resolves to instantly (no silent diversion)", async () => {
    // Same asymmetry `resolveTransportForSend` enforces everywhere else: the only
    // way onto the self-send pipe is an explicit, exact 'smtp'.
    seedReads({
      accounts: [
        {
          email: "typo@dfy.com",
          instantlyStatus: -1,
          warmupScore: 0,
          dailyLimit: 20,
          sendTransport: "SMTP ", // not exactly 'smtp'
          lifecycleStatus: "in_recovery",
        },
      ],
      delivery: [{ accountEmail: "typo@dfy.com", inboxCount: 98, seedTotal: 98 }],
    });

    const summary = await reconcileLifecycle("api-key");

    expect(summary.changed).toBe(1);
    const event = mockInsertValues.mock.calls[0][0] as Record<string, unknown>;
    expect(event.toStatus).toBe("deactivated_by_instantly");
  });
});
