import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import request from "supertest";
import express from "express";

// Mock DB — only used for campaign verification lookup. Bronze insert and silver
// promotion are mocked at the module level below.
const mockDbSelect = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: () => ({ where: mockDbSelect }) }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { instantlyCampaignId: "instantly_campaign_id" },
}));

// Bronze + silver are unit-tested separately. Webhook tests verify orchestration.
const mockInsertWebhookPayload = vi.fn();
const mockPromoteFromWebhookPayload = vi.fn();

vi.mock("../../src/lib/bronze", () => ({
  insertWebhookPayload: (...args: unknown[]) => mockInsertWebhookPayload(...args),
}));

vi.mock("../../src/lib/silver-promote", () => ({
  promoteFromWebhookPayload: (...args: unknown[]) =>
    mockPromoteFromWebhookPayload(...args),
}));

async function createWebhookApp() {
  const webhooksRouter = (await import("../../src/routes/webhooks")).default;
  const app = express();
  app.use(express.json());
  app.use("/webhooks", webhooksRouter);
  return app;
}

function mockVerification(instantlyCampaignId = "inst-camp-1") {
  mockDbSelect.mockResolvedValueOnce([
    {
      id: "camp-db-1",
      instantlyCampaignId,
      orgId: "org-uuid",
      runId: "run-1",
    },
  ]);
}

describe("POST /webhooks/instantly", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDbSelect.mockResolvedValue([]);
    mockInsertWebhookPayload.mockResolvedValue({ id: "bronze-row-1" });
    mockPromoteFromWebhookPayload.mockResolvedValue({
      promoted: true,
      silverEventId: "silver-event-1",
    });
  });

  it("should reject requests without campaign_id", async () => {
    const app = await createWebhookApp();
    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent" });

    expect(res.status).toBe(400);
    expect(res.body.error).toBe("Missing campaign_id");
  });

  it("should reject requests with unknown campaign_id", async () => {
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
    expect(res.body.bronzeRowId).toBe("bronze-row-1");
    expect(res.body.promoted).toBe(true);
  });

  it("should insert raw payload into bronze before promoting silver", async () => {
    mockVerification("inst-camp-1");
    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 2,
        variant: 1,
      });

    expect(mockInsertWebhookPayload).toHaveBeenCalledWith(
      "inst-camp-1",
      "org-uuid",
      expect.objectContaining({ event_type: "email_sent", step: 2 }),
    );
  });

  it("should promote silver after bronze insert with bronzeRowId attribution", async () => {
    mockVerification("inst-camp-1");
    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "reply_received",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
      });

    expect(mockPromoteFromWebhookPayload).toHaveBeenCalledWith(
      expect.objectContaining({
        bronzeRowId: "bronze-row-1",
        payload: expect.objectContaining({
          event_type: "reply_received",
          campaign_id: "inst-camp-1",
          lead_email: "lead@test.com",
        }),
      }),
    );
  });

  it("should pass step + variant from webhook payload through to silver promotion", async () => {
    mockVerification("inst-camp-1");
    const app = await createWebhookApp();

    await request(app)
      .post("/webhooks/instantly")
      .send({
        event_type: "email_sent",
        campaign_id: "inst-camp-1",
        lead_email: "lead@test.com",
        step: 2,
        variant: 1,
      });

    expect(mockPromoteFromWebhookPayload).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: expect.objectContaining({ step: 2, variant: 1 }),
      }),
    );
  });

  it("should return promoted=false when silver dedup detects duplicate", async () => {
    mockVerification("inst-camp-1");
    mockPromoteFromWebhookPayload.mockResolvedValue({
      promoted: false,
      silverEventId: null,
    });
    const app = await createWebhookApp();

    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent", campaign_id: "inst-camp-1" });

    expect(res.status).toBe(200);
    expect(res.body.promoted).toBe(false);
  });

  it("should return 500 if bronze insert fails", async () => {
    mockVerification("inst-camp-1");
    mockInsertWebhookPayload.mockRejectedValue(new Error("DB down"));
    const app = await createWebhookApp();

    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent", campaign_id: "inst-camp-1" });

    expect(res.status).toBe(500);
    expect(res.body.error).toBe("DB down");
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
    process.env.INSTANTLY_SERVICE_URL = "https://instantly.distribute.you";
    const app = await createWebhookApp();

    const res = await request(app).get("/webhooks/instantly/config");

    expect(res.status).toBe(200);
    expect(res.body.webhookUrl).toBe(
      "https://instantly.distribute.you/webhooks/instantly",
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
