import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import request from "supertest";
import express from "express";

// Mock DB
const mockDbInsert = vi.fn();
const mockDbSelect = vi.fn();
const mockDbUpdate = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({ values: (v: unknown) => { mockDbInsert(v); return Promise.resolve(); } }),
    select: () => ({ from: () => ({ where: mockDbSelect }) }),
    update: () => ({ set: (v: unknown) => ({ where: () => { mockDbUpdate(v); return Promise.resolve([{}]); } }) }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyEvents: {},
  instantlyCampaigns: {
    instantlyCampaignId: "instantly_campaign_id",
  },
  sequenceCosts: {
    id: "id",
    campaignId: "campaign_id",
    leadEmail: "lead_email",
    step: "step",
    status: "status",
  },
}));

// Mock runs-client
const mockUpdateCostStatus = vi.fn();
const mockUpdateRun = vi.fn();

vi.mock("../../src/lib/runs-client", () => ({
  updateCostStatus: (...args: unknown[]) => mockUpdateCostStatus(...args),
  updateRun: (...args: unknown[]) => mockUpdateRun(...args),
}));

async function createWebhookApp() {
  const webhooksRouter = (await import("../../src/routes/webhooks")).default;
  const app = express();
  app.use(express.json());
  app.use("/webhooks", webhooksRouter);
  return app;
}

/** Verification mock — returns a campaign for the campaign_id DB lookup */
function mockVerification(campaignId = "inst-camp-1") {
  mockDbSelect.mockResolvedValueOnce([{
    id: "camp-db-1",
    instantlyCampaignId: campaignId,
  }]);
}

describe("POST /webhooks/instantly", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDbSelect.mockResolvedValue([]);
    mockUpdateCostStatus.mockResolvedValue({});
    mockUpdateRun.mockResolvedValue({});
  });

  // ─── Verification tests ──────────────────────────────────────────────────

  it("should reject requests without campaign_id", async () => {
    const app = await createWebhookApp();

    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent" });

    expect(res.status).toBe(400);
    expect(res.body.error).toBe("Missing campaign_id");
  });

  it("should reject requests with unknown campaign_id", async () => {
    // Default mockDbSelect returns [] → campaign not found
    const app = await createWebhookApp();

    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent", campaign_id: "unknown-camp" });

    expect(res.status).toBe(401);
    expect(res.body.error).toBe("Unknown campaign_id");
  });

  it("should accept webhook with valid campaign_id", async () => {
    mockVerification("inst-camp-1");

    const app = await createWebhookApp();

    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent", campaign_id: "inst-camp-1" });

    expect(res.status).toBe(200);
    expect(res.body.eventType).toBe("email_sent");
  });

  // ─── Event recording tests ───────────────────────────────────────────────

  it("should store step and variant from webhook payload", async () => {
    mockVerification("camp-1");

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "email_sent",
        campaign_id: "camp-1",
        lead_email: "lead@test.com",
        email_account: "sender@test.com",
        step: 2,
        variant: 1,
      });

    expect(mockDbInsert).toHaveBeenCalledWith(
      expect.objectContaining({
        eventType: "email_sent",
        step: 2,
        variant: 1,
      }),
    );
  });

  // ─── Cost lifecycle tests ────────────────────────────────────────────────

  it("should convert provisioned cost to actual on email_sent for step > 1", async () => {
    // Mock: verification lookup
    mockVerification("inst-camp-1");
    // Mock: handleFollowUpSent → find the campaign
    mockDbSelect.mockResolvedValueOnce([{
      campaignId: "camp-1",
      instantlyCampaignId: "inst-camp-1",
    }]);
    // Mock: handleFollowUpSent → find the provisioned cost for this step
    mockDbSelect.mockResolvedValueOnce([{
      id: "sc-1",
      campaignId: "camp-1",
      leadEmail: "lead@test.com",
      step: 2,
      runId: "run-1",
      costId: "cost-2",
      status: "provisioned",
    }]);

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 2,
      });

    expect(mockUpdateCostStatus).toHaveBeenCalledWith("run-1", "cost-2", "actual");
    expect(mockUpdateRun).toHaveBeenCalledWith("run-1", "completed");
  });

  it("should NOT convert cost on email_sent for step 1 (already actual)", async () => {
    mockVerification("inst-camp-1");

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 1,
      });

    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });

  it("should cancel remaining provisions and fail step runs on reply_received", async () => {
    // Mock: verification lookup
    mockVerification("inst-camp-1");
    // Mock: cancelRemainingProvisions → find the campaign
    mockDbSelect.mockResolvedValueOnce([{
      campaignId: "camp-1",
      instantlyCampaignId: "inst-camp-1",
    }]);
    // Mock: cancelRemainingProvisions → find remaining provisioned costs
    mockDbSelect.mockResolvedValueOnce([
      { id: "sc-2", step: 2, runId: "step-run-2", costId: "cost-2", status: "provisioned" },
      { id: "sc-3", step: 3, runId: "step-run-3", costId: "cost-3", status: "provisioned" },
    ]);

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "reply_received",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockUpdateCostStatus).toHaveBeenCalledTimes(2);
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("step-run-2", "cost-2", "cancelled");
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("step-run-3", "cost-3", "cancelled");
    expect(mockUpdateRun).toHaveBeenCalledTimes(2);
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-2", "failed", "Sequence stopped: reply_received");
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-3", "failed", "Sequence stopped: reply_received");
  });

  it("should cancel provisions and fail step run on email_bounced", async () => {
    mockVerification("inst-camp-1");
    mockDbSelect.mockResolvedValueOnce([{ campaignId: "camp-1" }]);
    mockDbSelect.mockResolvedValueOnce([
      { id: "sc-2", step: 2, runId: "step-run-2", costId: "cost-2", status: "provisioned" },
    ]);

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "email_bounced",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockUpdateCostStatus).toHaveBeenCalledWith("step-run-2", "cost-2", "cancelled");
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-2", "failed", "Sequence stopped: email_bounced");
  });

  it("should cancel provisions and fail step run on lead_unsubscribed", async () => {
    mockVerification("inst-camp-1");
    mockDbSelect.mockResolvedValueOnce([{ campaignId: "camp-1" }]);
    mockDbSelect.mockResolvedValueOnce([
      { id: "sc-2", step: 2, runId: "step-run-2", costId: "cost-2", status: "provisioned" },
    ]);

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "lead_unsubscribed",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockUpdateCostStatus).toHaveBeenCalledWith("step-run-2", "cost-2", "cancelled");
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-2", "failed", "Sequence stopped: lead_unsubscribed");
  });

  it("should NOT cancel provisions on auto_reply_received (sequence continues)", async () => {
    mockVerification("inst-camp-1");

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "auto_reply_received",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });

  it("should NOT cancel provisions on lead_out_of_office (sequence continues)", async () => {
    mockVerification("inst-camp-1");

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "lead_out_of_office",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });

  // ─── Delivery status tests ───────────────────────────────────────────────

  it("should update deliveryStatus to 'sent' on email_sent", async () => {
    mockVerification("inst-camp-1");

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 1,
      });

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ deliveryStatus: "sent" }),
    );
  });

  it("should update deliveryStatus to 'replied' on reply_received", async () => {
    mockVerification("inst-camp-1");
    mockDbSelect.mockResolvedValueOnce([{ campaignId: "camp-1" }]);
    mockDbSelect.mockResolvedValueOnce([]);

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "reply_received",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ deliveryStatus: "replied" }),
    );
  });

  it("should update deliveryStatus to 'bounced' on email_bounced", async () => {
    mockVerification("inst-camp-1");
    mockDbSelect.mockResolvedValueOnce([{ campaignId: "camp-1" }]);
    mockDbSelect.mockResolvedValueOnce([]);

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "email_bounced",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ deliveryStatus: "bounced" }),
    );
  });

  it("should update deliveryStatus to 'unsubscribed' on lead_unsubscribed", async () => {
    mockVerification("inst-camp-1");
    mockDbSelect.mockResolvedValueOnce([{ campaignId: "camp-1" }]);
    mockDbSelect.mockResolvedValueOnce([]);

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "lead_unsubscribed",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ deliveryStatus: "unsubscribed" }),
    );
  });

  it("should update deliveryStatus to 'delivered' on campaign_completed", async () => {
    mockVerification("inst-camp-1");

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "campaign_completed",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ deliveryStatus: "delivered" }),
    );
  });

  it("should NOT update deliveryStatus for unknown event types", async () => {
    mockVerification("inst-camp-1");

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "auto_reply_received",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    // mockDbUpdate should NOT have been called with deliveryStatus
    const deliveryStatusCalls = mockDbUpdate.mock.calls.filter(
      ([v]: [any]) => v.deliveryStatus !== undefined,
    );
    expect(deliveryStatusCalls).toHaveLength(0);
  });
});

describe("GET /webhooks/instantly/config", () => {
  const originalDomain = process.env.INSTANTLY_SERVICE_URL;

  afterEach(() => {
    if (originalDomain !== undefined) {
      process.env.INSTANTLY_SERVICE_URL = originalDomain;
    } else {
      delete process.env.INSTANTLY_SERVICE_URL;
    }
  });

  it("should return webhookUrl from INSTANTLY_SERVICE_URL", async () => {
    process.env.INSTANTLY_SERVICE_URL = "https://instantly.distribute.org";

    const app = await createWebhookApp();

    const res = await request(app).get("/webhooks/instantly/config");

    expect(res.status).toBe(200);
    expect(res.body.webhookUrl).toBe(
      "https://instantly.distribute.org/webhooks/instantly",
    );
  });

  it("should return 500 when INSTANTLY_SERVICE_URL is not available", async () => {
    delete process.env.INSTANTLY_SERVICE_URL;

    const app = await createWebhookApp();

    const res = await request(app).get("/webhooks/instantly/config");

    expect(res.status).toBe(500);
    expect(res.body.error).toBe("INSTANTLY_SERVICE_URL not configured");
  });
});
