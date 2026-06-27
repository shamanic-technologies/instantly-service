import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbSelectFrom = vi.fn();
const mockDbExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: () => mockDbSelectFrom() }),
    execute: (...args: unknown[]) => mockDbExecute(...args),
    update: () => ({ set: () => ({ where: () => Promise.resolve() }) }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {
    id: "id",
    instantlyCampaignId: "instantly_campaign_id",
    orgId: "org_id",
  },
  instantlyEvents: {},
}));

const mockGetCampaign = vi.fn();
const mockGetCampaignAnalytics = vi.fn();
const mockListLeadsFull = vi.fn();
const mockListEmails = vi.fn();

const mockDeleteLeads = vi.fn();

vi.mock("../../src/lib/instantly-client", () => ({
  getCampaign: (...args: unknown[]) => mockGetCampaign(...args),
  getCampaignAnalytics: (...args: unknown[]) => mockGetCampaignAnalytics(...args),
  listLeadsFull: (...args: unknown[]) => mockListLeadsFull(...args),
  listEmails: (...args: unknown[]) => mockListEmails(...args),
  deleteLeads: (...args: unknown[]) => mockDeleteLeads(...args),
}));

const mockResolveInstantlyApiKey = vi.fn();

vi.mock("../../src/lib/key-client", () => {
  class KeyServiceError extends Error {
    constructor(public readonly statusCode: number, message: string) {
      super(message);
    }
  }
  return {
    resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
    KeyServiceError,
  };
});

// Reload the same class so the test can construct it
const { KeyServiceError } = await import("../../src/lib/key-client");

const mockInsertAnalyticsSnapshot = vi.fn();
const mockInsertCampaignConfigSnapshot = vi.fn();
const mockInsertEmailsBatch = vi.fn();
const mockInsertLeadsSnapshot = vi.fn();

vi.mock("../../src/lib/bronze", () => ({
  insertAnalyticsSnapshot: (...args: unknown[]) => mockInsertAnalyticsSnapshot(...args),
  insertCampaignConfigSnapshot: (...args: unknown[]) =>
    mockInsertCampaignConfigSnapshot(...args),
  insertEmailsBatch: (...args: unknown[]) => mockInsertEmailsBatch(...args),
  insertLeadsSnapshot: (...args: unknown[]) => mockInsertLeadsSnapshot(...args),
}));

const mockPromoteFromCampaignConfig = vi.fn();
const mockPromoteFromEmailRecord = vi.fn();
const mockPromoteFromLead = vi.fn();
const mockPromoteSyntheticOpensFromLead = vi.fn();
const mockPromoteSyntheticClicksFromLead = vi.fn();
const mockPromoteSyntheticInterestFromLead = vi.fn();

vi.mock("../../src/lib/silver-promote", () => ({
  promoteFromCampaignConfig: (...args: unknown[]) =>
    mockPromoteFromCampaignConfig(...args),
  promoteFromEmailRecord: (...args: unknown[]) => mockPromoteFromEmailRecord(...args),
  promoteFromLead: (...args: unknown[]) => mockPromoteFromLead(...args),
  promoteSyntheticOpensFromLead: (...args: unknown[]) =>
    mockPromoteSyntheticOpensFromLead(...args),
  promoteSyntheticClicksFromLead: (...args: unknown[]) =>
    mockPromoteSyntheticClicksFromLead(...args),
  promoteSyntheticInterestFromLead: (...args: unknown[]) =>
    mockPromoteSyntheticInterestFromLead(...args),
  cancelRemainingProvisions: (...args: unknown[]) =>
    mockCancelRemainingProvisions(...args),
}));

const mockCancelRemainingProvisions = vi.fn();

// Import under test AFTER mocks
import { reconcileAll } from "../../src/lib/reconcile";

function makeAnalytics(overrides: Record<string, number> = {}) {
  return {
    campaign_id: "inst-camp-1",
    campaign_name: "test",
    campaign_status: 1,
    leads_count: 1,
    contacted_count: 0,
    emails_sent_count: 0,
    new_leads_contacted_count: 0,
    open_count: 0,
    open_count_unique: 0,
    reply_count: 0,
    link_click_count: 0,
    bounced_count: 0,
    unsubscribed_count: 0,
    completed_count: 0,
    ...overrides,
  };
}

function mockLocalCounts(
  counts: { sent: number; replies: number; bounces: number; unsubs: number; opensUnique?: number },
) {
  mockDbExecute.mockImplementationOnce(() =>
    Promise.resolve({ rows: [{ opensUnique: 0, ...counts }] }),
  );
}

function mockEmailsCursor(cursor: string | null = null) {
  mockDbExecute.mockImplementationOnce(() =>
    Promise.resolve({ rows: [{ maxTs: cursor }] }),
  );
}

function mockEmailIdLookup(rows: Array<{ id: string; instantly_email_id: string }>) {
  mockDbExecute.mockImplementationOnce(() => Promise.resolve({ rows }));
}

describe("reconcileAll", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockResolveInstantlyApiKey.mockResolvedValue({ key: "fake-api-key", keySource: "platform" });
    mockGetCampaign.mockResolvedValue({ id: "inst-camp-1", not_sending_status: null });
    mockInsertAnalyticsSnapshot.mockResolvedValue({ id: "analytics-1" });
    mockInsertCampaignConfigSnapshot.mockResolvedValue({ id: "config-1" });
    mockInsertLeadsSnapshot.mockResolvedValue([]);
    mockInsertEmailsBatch.mockResolvedValue([]);
    mockPromoteFromCampaignConfig.mockResolvedValue({ promoted: false });
    mockPromoteFromEmailRecord.mockResolvedValue({ promoted: false, silverEventId: null });
    mockPromoteFromLead.mockResolvedValue({ promoted: false, silverEventId: null });
    mockPromoteSyntheticOpensFromLead.mockResolvedValue({ promoted: false });
    mockPromoteSyntheticClicksFromLead.mockResolvedValue({ promoted: false });
    mockPromoteSyntheticInterestFromLead.mockResolvedValue({ promoted: false });
  });

  it("returns zero summary when no campaigns exist", async () => {
    mockDbSelectFrom.mockResolvedValue([]);

    const result = await reconcileAll();

    expect(result.campaignsScanned).toBe(0);
    expect(result.campaignsWithDrift).toBe(0);
    expect(mockGetCampaignAnalytics).not.toHaveBeenCalled();
  });

  it("inserts bronze analytics snapshot for every campaign scanned", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-1" },
    ]);
    mockGetCampaignAnalytics.mockResolvedValue(makeAnalytics());
    mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });

    await reconcileAll();

    expect(mockInsertAnalyticsSnapshot).toHaveBeenCalledWith(
      "inst-1",
      "org-1",
      expect.objectContaining({ campaign_id: "inst-camp-1" }),
    );
  });

  it("skips phases 2-3 when aggregate matches local (no drift)", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-1" },
    ]);
    mockGetCampaignAnalytics.mockResolvedValue(
      makeAnalytics({ emails_sent_count: 1, reply_count: 0 }),
    );
    mockLocalCounts({ sent: 1, replies: 0, bounces: 0, unsubs: 0 });

    const result = await reconcileAll();

    expect(result.campaignsWithDrift).toBe(0);
    expect(mockListLeadsFull).not.toHaveBeenCalled();
    expect(mockListEmails).not.toHaveBeenCalled();
  });

  it("runs phases 2-3 when reply count drifts", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-1" },
    ]);
    mockGetCampaignAnalytics.mockResolvedValue(
      makeAnalytics({ emails_sent_count: 1, reply_count: 1 }),
    );
    mockLocalCounts({ sent: 1, replies: 0, bounces: 0, unsubs: 0 });
    mockListLeadsFull.mockResolvedValue([
      {
        id: "lead-1",
        email: "lead@test.com",
        status: 1,
        timestamp_last_reply: "2026-05-01T10:00:00Z",
        email_replied_step: 2,
      },
    ]);
    mockInsertLeadsSnapshot.mockResolvedValue([{ id: "bronze-lead-1" }]);
    mockPromoteFromLead.mockResolvedValue({ promoted: true, silverEventId: "silver-1" });
    mockEmailsCursor(null);
    mockListEmails.mockResolvedValue([
      {
        id: "email-1",
        campaign_id: "inst-1",
        lead: "lead@test.com",
        lead_id: "lead-id-1",
        eaccount: "sender@test.com",
        ue_type: 2,
        step: "step-1",
        timestamp_email: "2026-05-01T10:00:00Z",
      },
    ]);
    mockInsertEmailsBatch.mockResolvedValue([{ id: "bronze-email-1" }]);
    mockEmailIdLookup([{ id: "bronze-email-1", instantly_email_id: "email-1" }]);
    mockPromoteFromEmailRecord.mockResolvedValue({ promoted: true, silverEventId: "silver-2" });

    const result = await reconcileAll();

    expect(result.campaignsWithDrift).toBe(1);
    expect(result.drift.replies).toBeGreaterThanOrEqual(1);
    expect(mockListLeadsFull).toHaveBeenCalledWith("fake-api-key", "inst-1");
    expect(mockListEmails).toHaveBeenCalledWith(
      "fake-api-key",
      expect.objectContaining({ campaignId: "inst-1" }),
    );
  });

  it("groups campaigns by orgId and resolves one API key per org", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-A" },
      { id: "db-2", instantlyCampaignId: "inst-2", orgId: "org-A" },
      { id: "db-3", instantlyCampaignId: "inst-3", orgId: "org-B" },
    ]);
    mockGetCampaignAnalytics.mockResolvedValue(makeAnalytics());
    mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });
    mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });
    mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });

    await reconcileAll();

    expect(mockResolveInstantlyApiKey).toHaveBeenCalledTimes(2);
    expect(mockResolveInstantlyApiKey).toHaveBeenCalledWith(
      "org-A",
      "system",
      expect.any(Object),
    );
    expect(mockResolveInstantlyApiKey).toHaveBeenCalledWith(
      "org-B",
      "system",
      expect.any(Object),
    );
  });

  it("skips org with missing key (KeyServiceError 404)", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-without-key" },
    ]);
    mockResolveInstantlyApiKey.mockRejectedValue(new KeyServiceError(404, "key not found"));

    const result = await reconcileAll();

    expect(result.campaignsSkippedNoKey).toBe(1);
    expect(result.campaignsScanned).toBe(0);
    expect(mockGetCampaignAnalytics).not.toHaveBeenCalled();
  });

  it("continues after individual campaign failure", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-1" },
      { id: "db-2", instantlyCampaignId: "inst-2", orgId: "org-1" },
    ]);
    mockGetCampaignAnalytics
      .mockRejectedValueOnce(new Error("API timeout"))
      .mockResolvedValueOnce(makeAnalytics());
    mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });

    const result = await reconcileAll();

    expect(result.campaignsScanned).toBe(2);
    expect(result.campaignsFailed).toBe(1);
  });

  it("idempotent: re-running with no upstream changes yields zero drift", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-1" },
    ]);
    mockGetCampaignAnalytics.mockResolvedValue(
      makeAnalytics({ emails_sent_count: 2, reply_count: 1, open_count_unique: 1 }),
    );
    // Local already has all events from a previous run
    mockLocalCounts({ sent: 2, replies: 1, bounces: 0, unsubs: 0, opensUnique: 1 });

    const result = await reconcileAll();

    expect(result.campaignsWithDrift).toBe(0);
    expect(result.drift.replies).toBe(0);
    expect(mockListLeadsFull).not.toHaveBeenCalled();
  });

  it("detects drift on opens_unique even when sent/replies/bounces/unsubs match", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-1" },
    ]);
    mockGetCampaignAnalytics.mockResolvedValue(
      makeAnalytics({ emails_sent_count: 2, open_count_unique: 5 }),
    );
    // Local sent matches but only 2 distinct leads have opened silver events
    mockLocalCounts({ sent: 2, replies: 0, bounces: 0, unsubs: 0, opensUnique: 2 });
    mockEmailsCursor(null);
    mockEmailIdLookup([]);

    mockListLeadsFull.mockResolvedValue([]);
    mockInsertLeadsSnapshot.mockResolvedValue([]);
    mockListEmails.mockResolvedValue([]);

    const result = await reconcileAll();

    expect(result.campaignsWithDrift).toBe(1);
    expect(mockListLeadsFull).toHaveBeenCalledWith("fake-api-key", "inst-1");
  });

  it("RECONCILE_FORCE_PHASE_2=true bypasses drift gate and fires phase 2 anyway", async () => {
    const prev = process.env.RECONCILE_FORCE_PHASE_2;
    process.env.RECONCILE_FORCE_PHASE_2 = "true";

    try {
      mockDbSelectFrom.mockResolvedValue([
        { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-1" },
      ]);
      mockGetCampaignAnalytics.mockResolvedValue(
        makeAnalytics({ emails_sent_count: 2, reply_count: 1, open_count_unique: 1 }),
      );
      // Local matches remote exactly — no drift
      mockLocalCounts({ sent: 2, replies: 1, bounces: 0, unsubs: 0, opensUnique: 1 });
      mockEmailsCursor(null);
      mockEmailIdLookup([]);

      mockListLeadsFull.mockResolvedValue([]);
      mockInsertLeadsSnapshot.mockResolvedValue([]);
      mockListEmails.mockResolvedValue([]);

      await reconcileAll();

      // Phase 2 must fire despite no drift
      expect(mockListLeadsFull).toHaveBeenCalledWith("fake-api-key", "inst-1");
    } finally {
      if (prev === undefined) delete process.env.RECONCILE_FORCE_PHASE_2;
      else process.env.RECONCILE_FORCE_PHASE_2 = prev;
    }
  });

  it("phase 0: writes campaign config to bronze and promotes not_sending_status", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-1" },
    ]);
    mockGetCampaign.mockResolvedValue({
      id: "inst-1",
      name: "Campaign 1",
      not_sending_status: 4,
    });
    mockGetCampaignAnalytics.mockResolvedValue(makeAnalytics());
    mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });

    await reconcileAll();

    expect(mockInsertCampaignConfigSnapshot).toHaveBeenCalledWith(
      "inst-1",
      "org-1",
      expect.objectContaining({ not_sending_status: 4 }),
    );
    expect(mockPromoteFromCampaignConfig).toHaveBeenCalledWith(
      expect.objectContaining({
        instantlyCampaignId: "inst-1",
        notSendingStatus: 4,
      }),
    );
  });

  it("phase 0: maps absent not_sending_status to null", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-1" },
    ]);
    mockGetCampaign.mockResolvedValue({ id: "inst-1", name: "Campaign 1" });
    mockGetCampaignAnalytics.mockResolvedValue(makeAnalytics());
    mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });

    await reconcileAll();

    expect(mockPromoteFromCampaignConfig).toHaveBeenCalledWith(
      expect.objectContaining({ notSendingStatus: null }),
    );
  });

  it("phase 0: failure does not abort reconcile (phases 1-3 still run)", async () => {
    mockDbSelectFrom.mockResolvedValue([
      { id: "db-1", instantlyCampaignId: "inst-1", orgId: "org-1" },
    ]);
    mockGetCampaign.mockRejectedValue(new Error("instantly-api GET /campaigns/inst-1 failed: 500"));
    mockGetCampaignAnalytics.mockResolvedValue(makeAnalytics());
    mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });

    const result = await reconcileAll();

    expect(result.campaignsScanned).toBe(1);
    expect(result.campaignsFailed).toBe(0);
    expect(mockInsertCampaignConfigSnapshot).not.toHaveBeenCalled();
    expect(mockPromoteFromCampaignConfig).not.toHaveBeenCalled();
    expect(mockGetCampaignAnalytics).toHaveBeenCalledTimes(1);
  });

  // ─── Credit-leak fix: cancel provisioned holds on terminal Instantly status ──
  describe("cancels remaining provisioned holds when Instantly status is finished", () => {
    const originalDeleteFlag = process.env.DELETE_FINISHED_CONTACTS_ENABLED;
    afterEach(() => {
      if (originalDeleteFlag === undefined) delete process.env.DELETE_FINISHED_CONTACTS_ENABLED;
      else process.env.DELETE_FINISHED_CONTACTS_ENABLED = originalDeleteFlag;
    });

    it("cancels provisioned holds on a PAUSED (2) campaign even with delete OFF", async () => {
      delete process.env.DELETE_FINISHED_CONTACTS_ENABLED;
      mockDbSelectFrom.mockResolvedValue([
        {
          id: "db-1",
          instantlyCampaignId: "inst-1",
          orgId: "org-1",
          campaignId: "camp-1",
          userId: "user-1",
          leadEmail: "lead@example.com",
          status: "active",
        },
      ]);
      // status 2 = paused → finished. No drift → returns finish() after phase 1.
      mockGetCampaign.mockResolvedValue({ id: "inst-1", status: 2 });
      mockGetCampaignAnalytics.mockResolvedValue(makeAnalytics());
      mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });

      await reconcileAll();

      expect(mockCancelRemainingProvisions).toHaveBeenCalledWith(
        expect.objectContaining({ campaignId: "camp-1", orgId: "org-1", userId: "user-1" }),
        "lead@example.com",
      );
      // Delete OFF → contact is NOT deleted (credit refund is independent of quota reclaim).
      expect(mockDeleteLeads).not.toHaveBeenCalled();
    });

    it("cancels holds AND deletes the contact on a COMPLETED (3) campaign with delete ON", async () => {
      process.env.DELETE_FINISHED_CONTACTS_ENABLED = "true";
      mockDbSelectFrom.mockResolvedValue([
        {
          id: "db-1",
          instantlyCampaignId: "inst-1",
          orgId: "org-1",
          campaignId: "camp-1",
          userId: "user-1",
          leadEmail: "lead@example.com",
          status: "active",
        },
      ]);
      mockGetCampaign.mockResolvedValue({ id: "inst-1", status: 3 });
      mockGetCampaignAnalytics.mockResolvedValue(makeAnalytics());
      mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });

      await reconcileAll();

      expect(mockCancelRemainingProvisions).toHaveBeenCalledWith(
        expect.objectContaining({ campaignId: "camp-1" }),
        "lead@example.com",
      );
      expect(mockDeleteLeads).toHaveBeenCalledWith("fake-api-key", "inst-1", ["lead@example.com"]);
    });

    it("does NOT cancel holds on an ACTIVE (1) campaign", async () => {
      delete process.env.DELETE_FINISHED_CONTACTS_ENABLED;
      mockDbSelectFrom.mockResolvedValue([
        {
          id: "db-1",
          instantlyCampaignId: "inst-1",
          orgId: "org-1",
          campaignId: "camp-1",
          userId: "user-1",
          leadEmail: "lead@example.com",
          status: "active",
        },
      ]);
      mockGetCampaign.mockResolvedValue({ id: "inst-1", status: 1 });
      mockGetCampaignAnalytics.mockResolvedValue(makeAnalytics());
      mockLocalCounts({ sent: 0, replies: 0, bounces: 0, unsubs: 0 });

      await reconcileAll();

      expect(mockCancelRemainingProvisions).not.toHaveBeenCalled();
    });
  });
});
