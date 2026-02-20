import { describe, it, expect, vi, beforeEach } from "vitest";
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

vi.mock("../../src/lib/runs-client", () => ({
  updateCostStatus: (...args: unknown[]) => mockUpdateCostStatus(...args),
}));

process.env.INSTANTLY_WEBHOOK_SECRET = "test-secret";

async function createWebhookApp() {
  const webhooksRouter = (await import("../../src/routes/webhooks")).default;
  const app = express();
  app.use(express.json());
  app.use("/webhooks", webhooksRouter);
  return app;
}

describe("POST /webhooks/instantly", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDbSelect.mockResolvedValue([]);
    mockUpdateCostStatus.mockResolvedValue({});
  });

  it("should reject requests without valid secret", async () => {
    const app = await createWebhookApp();

    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent" });

    expect(res.status).toBe(401);
  });

  it("should accept secret in query param", async () => {
    const app = await createWebhookApp();

    const res = await request(app)
      .post("/webhooks/instantly?secret=test-secret")
      .send({ event_type: "email_sent" });

    expect(res.status).toBe(200);
    expect(res.body.eventType).toBe("email_sent");
  });

  it("should store step and variant from webhook payload", async () => {
    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly?secret=test-secret")
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

  it("should convert provisioned cost to actual on email_sent for step > 1", async () => {
    // Mock: find the campaign
    mockDbSelect.mockResolvedValueOnce([{
      campaignId: "camp-1",
      instantlyCampaignId: "inst-camp-1",
    }]);
    // Mock: find the provisioned cost for this step
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
      .post("/webhooks/instantly?secret=test-secret")
      .send({
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 2,
      });

    expect(mockUpdateCostStatus).toHaveBeenCalledWith("run-1", "cost-2", "actual");
  });

  it("should NOT convert cost on email_sent for step 1 (already actual)", async () => {
    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly?secret=test-secret")
      .send({
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 1,
      });

    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });

  it("should cancel remaining provisions on reply_received", async () => {
    // Mock: find the campaign
    mockDbSelect.mockResolvedValueOnce([{
      campaignId: "camp-1",
      instantlyCampaignId: "inst-camp-1",
    }]);
    // Mock: find remaining provisioned costs
    mockDbSelect.mockResolvedValueOnce([
      { id: "sc-2", step: 2, runId: "run-1", costId: "cost-2", status: "provisioned" },
      { id: "sc-3", step: 3, runId: "run-1", costId: "cost-3", status: "provisioned" },
    ]);

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly?secret=test-secret")
      .send({
        event_type: "reply_received",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockUpdateCostStatus).toHaveBeenCalledTimes(2);
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("run-1", "cost-2", "cancelled");
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("run-1", "cost-3", "cancelled");
  });

  it("should cancel provisions on email_bounced", async () => {
    mockDbSelect.mockResolvedValueOnce([{ campaignId: "camp-1" }]);
    mockDbSelect.mockResolvedValueOnce([
      { id: "sc-2", step: 2, runId: "run-1", costId: "cost-2", status: "provisioned" },
    ]);

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly?secret=test-secret")
      .send({
        event_type: "email_bounced",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockUpdateCostStatus).toHaveBeenCalledWith("run-1", "cost-2", "cancelled");
  });

  it("should cancel provisions on lead_unsubscribed", async () => {
    mockDbSelect.mockResolvedValueOnce([{ campaignId: "camp-1" }]);
    mockDbSelect.mockResolvedValueOnce([
      { id: "sc-2", step: 2, runId: "run-1", costId: "cost-2", status: "provisioned" },
    ]);

    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly?secret=test-secret")
      .send({
        event_type: "lead_unsubscribed",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockUpdateCostStatus).toHaveBeenCalledWith("run-1", "cost-2", "cancelled");
  });

  it("should NOT cancel provisions on auto_reply_received (sequence continues)", async () => {
    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly?secret=test-secret")
      .send({
        event_type: "auto_reply_received",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });

  it("should NOT cancel provisions on lead_out_of_office (sequence continues)", async () => {
    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly?secret=test-secret")
      .send({
        event_type: "lead_out_of_office",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });
});
