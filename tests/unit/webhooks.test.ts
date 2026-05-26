import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import request from "supertest";
import express from "express";

// Mock DB — only used for campaign verification lookup. Bronze insert and silver
// promotion are mocked at the module level below.
const mockDbSelect = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({
      from: () => ({
        where: (...whereArgs: unknown[]) => {
          const resultPromise = Promise.resolve(mockDbSelect(...whereArgs));
          return Object.assign(resultPromise, {
            limit: () => resultPromise,
          });
        },
      }),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { instantlyCampaignId: "instantly_campaign_id", metadata: "metadata" },
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

  it("should return 200 + degraded for unknown campaign_id (avoid Instantly auto-pause)", async () => {
    const app = await createWebhookApp();
    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent", campaign_id: "unknown-camp" });

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.degraded).toBe(true);
    expect(res.body.degradedReason).toBe("unknown_campaign_id");
    expect(res.body.bronzeRowId).toBeNull();
    expect(res.body.promoted).toBe(false);
    // Bronze + silver MUST be skipped — no campaign row to anchor to
    expect(mockInsertWebhookPayload).not.toHaveBeenCalled();
    expect(mockPromoteFromWebhookPayload).not.toHaveBeenCalled();
  });

  it("should find redispatched campaign via metadata.redispatchHistory alias", async () => {
    // Webhook arrives with OLD instantly_campaign_id, but row's current id is NEW.
    // The DB lookup uses an OR (current id == X OR metadata @? alias path).
    // We mock the lookup to return the row (matched via alias).
    mockDbSelect.mockResolvedValueOnce([
      {
        id: "camp-db-1",
        instantlyCampaignId: "inst-camp-NEW",
        orgId: "org-uuid",
        runId: "run-1",
        metadata: {
          redispatchHistory: [{ from: "inst-camp-OLD", to: "inst-camp-NEW", account: "x", at: "..." }],
        },
      },
    ]);
    const app = await createWebhookApp();
    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_opened", campaign_id: "inst-camp-OLD" });

    expect(res.status).toBe(200);
    expect(res.body.degraded).toBe(false);
    expect(res.body.bronzeRowId).toBe("bronze-row-1");
    expect(res.body.promoted).toBe(true);
    // Bronze + silver write under the canonical (current) id, NOT the alias
    expect(mockInsertWebhookPayload).toHaveBeenCalledWith(
      "inst-camp-NEW",
      "org-uuid",
      expect.objectContaining({ campaign_id: "inst-camp-OLD" }),
    );
    expect(mockPromoteFromWebhookPayload).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: expect.objectContaining({ campaign_id: "inst-camp-NEW" }),
      }),
    );
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

  it("should return 200 with degraded=true when bronze insert fails (avoid Instantly auto-pause)", async () => {
    mockVerification("inst-camp-1");
    mockInsertWebhookPayload.mockRejectedValue(new Error("DB down"));
    const app = await createWebhookApp();

    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent", campaign_id: "inst-camp-1" });

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.degraded).toBe(true);
    expect(res.body.degradedReason).toContain("bronze failed");
    expect(res.body.degradedReason).toContain("DB down");
    expect(res.body.bronzeRowId).toBeNull();
    expect(res.body.promoted).toBe(false);
    // Silver MUST be skipped when bronze fails — no bronzeRowId to anchor to
    expect(mockPromoteFromWebhookPayload).not.toHaveBeenCalled();
  });

  it("should return 200 with degraded=true when silver promote fails (avoid Instantly auto-pause)", async () => {
    mockVerification("inst-camp-1");
    mockPromoteFromWebhookPayload.mockRejectedValue(new Error("constraint violation"));
    const app = await createWebhookApp();

    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent", campaign_id: "inst-camp-1" });

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.degraded).toBe(true);
    expect(res.body.degradedReason).toContain("silver failed");
    expect(res.body.degradedReason).toContain("constraint violation");
    // Bronze write still succeeded — row IS persisted for reconcile catch-up
    expect(res.body.bronzeRowId).toBe("bronze-row-1");
    expect(res.body.promoted).toBe(false);
  });

  it("happy path returns 200 with degraded=false", async () => {
    mockVerification("inst-camp-1");
    const app = await createWebhookApp();

    const res = await request(app)
      .post("/webhooks/instantly")
      .send({ event_type: "email_sent", campaign_id: "inst-camp-1" });

    expect(res.status).toBe(200);
    expect(res.body.degraded).toBe(false);
    expect(res.body.degradedReason).toBeNull();
    expect(res.body.bronzeRowId).toBe("bronze-row-1");
    expect(res.body.promoted).toBe(true);
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
