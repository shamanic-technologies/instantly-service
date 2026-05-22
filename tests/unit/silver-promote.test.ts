import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbSelect = vi.fn();
const mockDbUpdate = vi.fn();
const mockDbInsertReturning = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: () => ({ where: mockDbSelect }) }),
    insert: () => ({
      values: () => ({
        onConflictDoNothing: () => ({
          returning: () => mockDbInsertReturning(),
        }),
      }),
    }),
    update: () => ({
      set: (v: unknown) => ({
        where: () => {
          mockDbUpdate(v);
          return Promise.resolve([{}]);
        },
      }),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { instantlyCampaignId: "instantly_campaign_id" },
  instantlyEvents: { id: "id" },
  sequenceCosts: {
    id: "id",
    campaignId: "campaign_id",
    leadEmail: "lead_email",
    step: "step",
    status: "status",
  },
}));

const mockUpdateCostStatus = vi.fn();
vi.mock("../../src/lib/runs-client", () => ({
  updateCostStatus: (...args: unknown[]) => mockUpdateCostStatus(...args),
}));

// Import under test AFTER mocks
import {
  promoteFromWebhookPayload,
  promoteFromEmailRecord,
  promoteFromLead,
  promoteSyntheticOpensFromLead,
  promoteFromCampaignConfig,
} from "../../src/lib/silver-promote";

const NIL_USER_UUID = "00000000-0000-0000-0000-000000000000";

function mockCampaign(extra: Record<string, unknown> = {}) {
  mockDbSelect.mockResolvedValueOnce([
    {
      campaignId: "camp-1",
      instantlyCampaignId: "inst-camp-1",
      orgId: "org-uuid",
      userId: "user-uuid",
      runId: "run-1",
      ...extra,
    },
  ]);
}

function mockProvisions(...costs: Array<Record<string, unknown>>) {
  mockDbSelect.mockResolvedValueOnce(costs);
}

function mockNewSilverRow(id = "silver-1") {
  mockDbInsertReturning.mockResolvedValueOnce([{ id }]);
}

function mockDuplicateSilverRow() {
  mockDbInsertReturning.mockResolvedValueOnce([]);
}

describe("promoteFromWebhookPayload", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockDbSelect.mockResolvedValue([]);
    mockDbInsertReturning.mockResolvedValue([]);
    mockUpdateCostStatus.mockResolvedValue({});
  });

  it("returns promoted=false when campaign not found", async () => {
    const result = await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: { event_type: "email_sent", campaign_id: "unknown" },
    });
    expect(result.promoted).toBe(false);
  });

  it("inserts silver event row and returns silverEventId on first insert", async () => {
    mockCampaign();
    mockNewSilverRow("silver-1");

    const result = await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 1,
      },
    });

    expect(result.promoted).toBe(true);
    expect(result.silverEventId).toBe("silver-1");
  });

  it("returns promoted=false when silver dedup hits unique index", async () => {
    mockCampaign();
    mockDuplicateSilverRow();

    const result = await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 1,
      },
    });

    expect(result.promoted).toBe(false);
    expect(result.silverEventId).toBeNull();
    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });

  it("updates deliveryStatus to 'sent' on email_sent", async () => {
    mockCampaign();
    mockNewSilverRow();

    await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 1,
      },
    });

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ deliveryStatus: "sent" }),
    );
  });

  it("updates deliveryStatus to 'replied' on reply_received", async () => {
    mockCampaign();
    mockNewSilverRow();
    mockProvisions(); // cancelRemainingProvisions: no costs

    await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "reply_received",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      },
    });

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ deliveryStatus: "replied" }),
    );
  });

  it("updates replyClassification to 'positive' on lead_interested", async () => {
    mockCampaign();
    mockNewSilverRow();

    await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "lead_interested",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      },
    });

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ replyClassification: "positive" }),
    );
  });

  it("updates replyClassification to 'negative' on lead_not_interested", async () => {
    mockCampaign();
    mockNewSilverRow();
    mockProvisions();

    await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "lead_not_interested",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      },
    });

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ replyClassification: "negative" }),
    );
  });

  it("converts provisioned cost to actual on email_sent step > 1", async () => {
    mockCampaign();
    mockNewSilverRow();
    mockProvisions({
      id: "sc-1",
      campaignId: "camp-1",
      leadEmail: "lead@test.com",
      step: 2,
      runId: "step-run-2",
      costId: "cost-2",
      status: "provisioned",
    });

    await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 2,
      },
    });

    expect(mockUpdateCostStatus).toHaveBeenCalledWith(
      "step-run-2",
      "cost-2",
      "actual",
      expect.objectContaining({ runId: "step-run-2", userId: "user-uuid" }),
    );
  });

  it("uses nil UUID when campaign has no userId", async () => {
    mockCampaign({ userId: null });
    mockNewSilverRow();
    mockProvisions({
      id: "sc-1",
      campaignId: "camp-1",
      leadEmail: "lead@test.com",
      step: 2,
      runId: "step-run-2",
      costId: "cost-2",
      status: "provisioned",
    });

    await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 2,
      },
    });

    expect(mockUpdateCostStatus).toHaveBeenCalledWith(
      "step-run-2",
      "cost-2",
      "actual",
      expect.objectContaining({ userId: NIL_USER_UUID }),
    );
  });

  it("does NOT convert cost on email_sent step 1", async () => {
    mockCampaign();
    mockNewSilverRow();

    await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 1,
      },
    });

    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });

  it("cancels remaining provisions on reply_received", async () => {
    mockCampaign();
    mockNewSilverRow();
    mockProvisions(
      { id: "sc-2", step: 2, runId: "step-run-2", costId: "cost-2", status: "provisioned" },
      { id: "sc-3", step: 3, runId: "step-run-3", costId: "cost-3", status: "provisioned" },
    );

    await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "reply_received",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      },
    });

    expect(mockUpdateCostStatus).toHaveBeenCalledTimes(2);
    expect(mockUpdateCostStatus).toHaveBeenCalledWith(
      "step-run-2",
      "cost-2",
      "cancelled",
      expect.objectContaining({ runId: "step-run-2" }),
    );
    expect(mockUpdateCostStatus).toHaveBeenCalledWith(
      "step-run-3",
      "cost-3",
      "cancelled",
      expect.objectContaining({ runId: "step-run-3" }),
    );
  });

  it("cancels provisions on email_bounced", async () => {
    mockCampaign();
    mockNewSilverRow();
    mockProvisions({
      id: "sc-2",
      step: 2,
      runId: "step-run-2",
      costId: "cost-2",
      status: "provisioned",
    });

    await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "email_bounced",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      },
    });

    expect(mockUpdateCostStatus).toHaveBeenCalledWith(
      "step-run-2",
      "cost-2",
      "cancelled",
      expect.objectContaining({ runId: "step-run-2" }),
    );
  });

  it("does NOT cancel provisions on auto_reply_received", async () => {
    mockCampaign();
    mockNewSilverRow();

    await promoteFromWebhookPayload({
      bronzeRowId: "bronze-1",
      payload: {
        event_type: "auto_reply_received",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      },
    });

    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });
});

describe("promoteFromEmailRecord", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockDbSelect.mockResolvedValue([]);
    mockDbInsertReturning.mockResolvedValue([]);
    mockUpdateCostStatus.mockResolvedValue({});
  });

  it("maps ue_type=1 to email_sent and parses step from 'step-2'", async () => {
    mockCampaign();
    mockNewSilverRow();
    mockProvisions({
      id: "sc-1",
      campaignId: "camp-1",
      leadEmail: "lead@test.com",
      step: 2,
      runId: "step-run-2",
      costId: "cost-2",
      status: "provisioned",
    });

    const result = await promoteFromEmailRecord({
      bronzeRowId: "bronze-email-1",
      email: {
        id: "email-1",
        campaign_id: "inst-camp-1",
        lead: "lead@test.com",
        lead_id: "lead-id-1",
        eaccount: "sender@test.com",
        ue_type: 1,
        step: "step-2",
        timestamp_email: "2026-05-01T10:00:00Z",
      },
    });

    expect(result.promoted).toBe(true);
    expect(mockUpdateCostStatus).toHaveBeenCalledWith(
      "step-run-2",
      "cost-2",
      "actual",
      expect.any(Object),
    );
  });

  it("maps ue_type=2 to reply_received and cancels provisions", async () => {
    mockCampaign();
    mockNewSilverRow();
    mockProvisions({
      id: "sc-2",
      step: 2,
      runId: "step-run-2",
      costId: "cost-2",
      status: "provisioned",
    });

    const result = await promoteFromEmailRecord({
      bronzeRowId: "bronze-email-1",
      email: {
        id: "email-1",
        campaign_id: "inst-camp-1",
        lead: "lead@test.com",
        lead_id: "lead-id-1",
        eaccount: "sender@test.com",
        ue_type: 2,
        step: "step-1",
        timestamp_email: "2026-05-01T10:00:00Z",
      },
    });

    expect(result.promoted).toBe(true);
    expect(mockUpdateCostStatus).toHaveBeenCalledWith(
      "step-run-2",
      "cost-2",
      "cancelled",
      expect.any(Object),
    );
  });

  it("skips ue_type=3 (manual sent) and ue_type=4 (scheduled)", async () => {
    const skipped3 = await promoteFromEmailRecord({
      bronzeRowId: "bronze-1",
      email: {
        id: "email-1",
        campaign_id: "inst-camp-1",
        lead: "lead@test.com",
        lead_id: "lead-id-1",
        eaccount: "sender@test.com",
        ue_type: 3,
        step: "step-1",
        timestamp_email: "2026-05-01T10:00:00Z",
      },
    });
    expect(skipped3.promoted).toBe(false);

    const skipped4 = await promoteFromEmailRecord({
      bronzeRowId: "bronze-1",
      email: {
        id: "email-2",
        campaign_id: "inst-camp-1",
        lead: "lead@test.com",
        lead_id: "lead-id-1",
        eaccount: "sender@test.com",
        ue_type: 4,
        step: "step-1",
        timestamp_email: "2026-05-01T10:00:00Z",
      },
    });
    expect(skipped4.promoted).toBe(false);
  });

  it("parses bare integer step '1' (no 'step-' prefix)", async () => {
    mockCampaign();
    mockNewSilverRow();

    const result = await promoteFromEmailRecord({
      bronzeRowId: "bronze-1",
      email: {
        id: "email-1",
        campaign_id: "inst-camp-1",
        lead: "lead@test.com",
        lead_id: "lead-id-1",
        eaccount: "sender@test.com",
        ue_type: 1,
        step: "1",
        timestamp_email: "2026-05-01T10:00:00Z",
      },
    });

    expect(result.promoted).toBe(true);
    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });
});

describe("promoteFromLead", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockDbSelect.mockResolvedValue([]);
    mockDbInsertReturning.mockResolvedValue([]);
    mockUpdateCostStatus.mockResolvedValue({});
  });

  it("derives email_bounced silver event from lead.status=-1", async () => {
    mockCampaign();
    mockNewSilverRow();
    mockProvisions({
      id: "sc-2",
      step: 2,
      runId: "step-run-2",
      costId: "cost-2",
      status: "provisioned",
    });

    const result = await promoteFromLead({
      bronzeRowId: "bronze-lead-1",
      instantlyCampaignId: "inst-camp-1",
      lead: {
        id: "lead-id-1",
        email: "bounced@test.com",
        status: -1,
        timestamp_last_contact: "2026-05-01T10:00:00Z",
      },
    });

    expect(result.promoted).toBe(true);
    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ deliveryStatus: "bounced" }),
    );
    expect(mockUpdateCostStatus).toHaveBeenCalledWith(
      "step-run-2",
      "cost-2",
      "cancelled",
      expect.any(Object),
    );
  });

  it("derives lead_unsubscribed silver event from lead.status=-2", async () => {
    mockCampaign();
    mockNewSilverRow();
    mockProvisions();

    const result = await promoteFromLead({
      bronzeRowId: "bronze-lead-1",
      instantlyCampaignId: "inst-camp-1",
      lead: {
        id: "lead-id-1",
        email: "unsub@test.com",
        status: -2,
        timestamp_last_contact: "2026-05-01T10:00:00Z",
      },
    });

    expect(result.promoted).toBe(true);
    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ deliveryStatus: "unsubscribed" }),
    );
  });

  it("derives reply_received from non-null timestamp_last_reply when status is active", async () => {
    mockCampaign();
    mockNewSilverRow();
    mockProvisions();

    const result = await promoteFromLead({
      bronzeRowId: "bronze-lead-1",
      instantlyCampaignId: "inst-camp-1",
      lead: {
        id: "lead-id-1",
        email: "replied@test.com",
        status: 1,
        timestamp_last_reply: "2026-05-01T10:00:00Z",
        email_replied_step: 2,
      },
    });

    expect(result.promoted).toBe(true);
    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ deliveryStatus: "replied" }),
    );
  });

  it("returns promoted=false when lead is active with no reply", async () => {
    const result = await promoteFromLead({
      bronzeRowId: "bronze-lead-1",
      instantlyCampaignId: "inst-camp-1",
      lead: {
        id: "lead-id-1",
        email: "active@test.com",
        status: 1,
      },
    });

    expect(result.promoted).toBe(false);
  });
});

describe("promoteFromCampaignConfig", () => {
  const FIFTEEN_MIN_MS = 15 * 60 * 1000;

  beforeEach(() => {
    vi.resetAllMocks();
    mockDbSelect.mockResolvedValue([]);
  });

  it("returns promoted=false when campaign row not found", async () => {
    mockDbSelect.mockResolvedValueOnce([]);

    const result = await promoteFromCampaignConfig({
      bronzeRowId: "bronze-cfg-1",
      instantlyCampaignId: "inst-camp-unknown",
      notSendingStatus: 4,
    });

    expect(result.promoted).toBe(false);
    expect(mockDbUpdate).not.toHaveBeenCalled();
  });

  it("promotes on first observation (current value null)", async () => {
    mockDbSelect.mockResolvedValueOnce([
      { notSendingStatus: null, notSendingStatusSeenAt: null },
    ]);
    const now = new Date("2026-05-22T12:00:00Z");

    const result = await promoteFromCampaignConfig({
      bronzeRowId: "bronze-cfg-1",
      instantlyCampaignId: "inst-camp-1",
      notSendingStatus: 4,
      now,
    });

    expect(result.promoted).toBe(true);
    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        notSendingStatus: 4,
        notSendingStatusSeenAt: now,
      }),
    );
  });

  it("skips when value unchanged and seen_at is within 15min window", async () => {
    const now = new Date("2026-05-22T12:00:00Z");
    const recent = new Date(now.getTime() - FIFTEEN_MIN_MS + 60_000); // 14min ago
    mockDbSelect.mockResolvedValueOnce([
      { notSendingStatus: 4, notSendingStatusSeenAt: recent },
    ]);

    const result = await promoteFromCampaignConfig({
      bronzeRowId: "bronze-cfg-1",
      instantlyCampaignId: "inst-camp-1",
      notSendingStatus: 4,
      now,
    });

    expect(result.promoted).toBe(false);
    expect(mockDbUpdate).not.toHaveBeenCalled();
  });

  it("promotes when value unchanged but seen_at is older than 15min", async () => {
    const now = new Date("2026-05-22T12:00:00Z");
    const stale = new Date(now.getTime() - FIFTEEN_MIN_MS - 60_000); // 16min ago
    mockDbSelect.mockResolvedValueOnce([
      { notSendingStatus: 4, notSendingStatusSeenAt: stale },
    ]);

    const result = await promoteFromCampaignConfig({
      bronzeRowId: "bronze-cfg-1",
      instantlyCampaignId: "inst-camp-1",
      notSendingStatus: 4,
      now,
    });

    expect(result.promoted).toBe(true);
    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        notSendingStatus: 4,
        notSendingStatusSeenAt: now,
      }),
    );
  });

  it("promotes when value changes (4 → null)", async () => {
    const now = new Date("2026-05-22T12:00:00Z");
    mockDbSelect.mockResolvedValueOnce([
      {
        notSendingStatus: 4,
        notSendingStatusSeenAt: new Date(now.getTime() - 60_000),
      },
    ]);

    const result = await promoteFromCampaignConfig({
      bronzeRowId: "bronze-cfg-1",
      instantlyCampaignId: "inst-camp-1",
      notSendingStatus: null,
      now,
    });

    expect(result.promoted).toBe(true);
    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        notSendingStatus: null,
        notSendingStatusSeenAt: now,
      }),
    );
  });

  it("promotes when value changes (null → 4) even with recent seen_at", async () => {
    const now = new Date("2026-05-22T12:00:00Z");
    mockDbSelect.mockResolvedValueOnce([
      {
        notSendingStatus: null,
        notSendingStatusSeenAt: new Date(now.getTime() - 60_000),
      },
    ]);

    const result = await promoteFromCampaignConfig({
      bronzeRowId: "bronze-cfg-1",
      instantlyCampaignId: "inst-camp-1",
      notSendingStatus: 4,
      now,
    });

    expect(result.promoted).toBe(true);
    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ notSendingStatus: 4 }),
    );
  });
});

describe("promoteSyntheticOpensFromLead", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDbSelect.mockResolvedValue([]);
    mockDbInsertReturning.mockResolvedValue([]);
  });

  it("inserts synthetic email_opened when open_count > 0", async () => {
    mockCampaign();
    mockNewSilverRow();

    const result = await promoteSyntheticOpensFromLead({
      bronzeRowId: "bronze-lead-1",
      instantlyCampaignId: "inst-camp-1",
      lead: {
        id: "lead-id-1",
        email: "opener@test.com",
        email_open_count: 3,
        email_opened_step: 2,
        timestamp_last_open: "2026-05-01T10:00:00Z",
      },
    });

    expect(result.promoted).toBe(true);
  });

  it("skips when open_count is 0", async () => {
    const result = await promoteSyntheticOpensFromLead({
      bronzeRowId: "bronze-lead-1",
      instantlyCampaignId: "inst-camp-1",
      lead: {
        id: "lead-id-1",
        email: "noopen@test.com",
        email_open_count: 0,
      },
    });

    expect(result.promoted).toBe(false);
  });

  it("skips when timestamp_last_open is null", async () => {
    const result = await promoteSyntheticOpensFromLead({
      bronzeRowId: "bronze-lead-1",
      instantlyCampaignId: "inst-camp-1",
      lead: {
        id: "lead-id-1",
        email: "nots@test.com",
        email_open_count: 1,
        timestamp_last_open: null,
      },
    });

    expect(result.promoted).toBe(false);
  });
});
