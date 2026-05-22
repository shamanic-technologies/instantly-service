/**
 * End-to-end integration test for the retry-stuck retro endpoint.
 *
 * Seeds real fixture rows in the local test DB (`instantly_campaigns`,
 * `sequence_costs`), stubs the Instantly + runs-service HTTP boundaries,
 * fires `POST /internal/campaigns/retry-stuck-now { all: true }`, and asserts
 * the rows flip to `delivery_status='cancelled'` while sequence costs go to
 * `status='cancelled'`. Skipped when no DB URL is configured (CI without
 * Neon access still runs unit tests via tests/unit/retry-stuck-route).
 */
import { describe, it, expect, vi, beforeEach, afterAll } from "vitest";
import request from "supertest";
import { eq } from "drizzle-orm";

// Mock the outbound HTTP clients BEFORE importing the test app. The DB itself
// is real so the SQL update in handleCampaignError + retry-stuck observe the
// fixture rows.
vi.mock("../../src/lib/instantly-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/lib/instantly-client")>();
  return {
    ...actual,
    getCampaign: vi.fn(),
    updateCampaignStatus: vi.fn(),
  };
});

vi.mock("../../src/lib/runs-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/lib/runs-client")>();
  return {
    ...actual,
    updateRun: vi.fn().mockResolvedValue({}),
    updateCostStatus: vi.fn().mockResolvedValue({}),
  };
});

vi.mock("../../src/lib/key-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/lib/key-client")>();
  return {
    ...actual,
    resolveInstantlyApiKey: vi.fn().mockResolvedValue({ key: "fake-key", keySource: "platform" }),
  };
});

vi.mock("../../src/lib/email-client", () => ({
  sendEmail: vi.fn().mockResolvedValue({}),
  deployTemplates: vi.fn().mockResolvedValue({}),
}));

import { createTestApp, getAuthHeaders } from "../helpers/test-app";
import { cleanTestData, closeDb } from "../helpers/test-db";
import { db } from "../../src/db";
import { instantlyCampaigns, sequenceCosts } from "../../src/db/schema";
import { getCampaign } from "../../src/lib/instantly-client";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;

describe.skipIf(SKIP)("POST /internal/campaigns/retry-stuck-now (DB-backed)", () => {
  const app = createTestApp();

  beforeEach(async () => {
    await cleanTestData();
    vi.clearAllMocks();
  });

  afterAll(async () => {
    await cleanTestData();
    await closeDb();
  });

  it("cancels every stuck fixture row when called with { all: true }", async () => {
    // Seed two campaigns both stuck in 'contacted' state.
    const baseCampaign = {
      campaignId: "camp-1",
      orgId: "org-1",
      userId: "00000000-0000-0000-0000-000000000001",
      brandIds: ["brand-1"],
      status: "active",
      deliveryStatus: "contacted",
      name: "test",
    };

    await db.insert(instantlyCampaigns).values([
      { ...baseCampaign, leadEmail: "stuck-a@test.com", instantlyCampaignId: "inst-a" },
      { ...baseCampaign, leadEmail: "stuck-b@test.com", instantlyCampaignId: "inst-b" },
    ]);

    await db.insert(sequenceCosts).values([
      { campaignId: "camp-1", leadEmail: "stuck-a@test.com", step: 1, runId: "run-a-1", costId: "cost-a-1", status: "provisioned" },
      { campaignId: "camp-1", leadEmail: "stuck-b@test.com", step: 1, runId: "run-b-1", costId: "cost-b-1", status: "provisioned" },
    ]);

    // Instantly reports not_sending_status for both.
    vi.mocked(getCampaign).mockResolvedValue({ not_sending_status: { reason: "esp_mismatch" } } as any);

    const res = await request(app)
      .post("/internal/campaigns/retry-stuck-now")
      .set(getAuthHeaders())
      .send({ all: true });

    expect(res.status).toBe(200);
    expect(res.body.cancelled).toBe(2);
    expect(res.body.scanned).toBe(2);

    const [rowA] = await db.select().from(instantlyCampaigns).where(eq(instantlyCampaigns.instantlyCampaignId, "inst-a"));
    const [rowB] = await db.select().from(instantlyCampaigns).where(eq(instantlyCampaigns.instantlyCampaignId, "inst-b"));
    expect(rowA.deliveryStatus).toBe("cancelled");
    expect(rowB.deliveryStatus).toBe("cancelled");
    expect(rowA.status).toBe("error");
    expect((rowA.metadata as Record<string, unknown>).retryCount).toBe(1);

    const allCosts = await db.select().from(sequenceCosts);
    for (const c of allCosts) {
      expect(c.status).toBe("cancelled");
    }
  });

  it("leaves rows alone when not_sending_status is null", async () => {
    await db.insert(instantlyCampaigns).values({
      campaignId: "camp-1",
      orgId: "org-1",
      userId: "00000000-0000-0000-0000-000000000001",
      brandIds: ["brand-1"],
      status: "active",
      deliveryStatus: "contacted",
      name: "test",
      leadEmail: "healthy@test.com",
      instantlyCampaignId: "inst-healthy",
    });

    vi.mocked(getCampaign).mockResolvedValue({ not_sending_status: null } as any);

    const res = await request(app)
      .post("/internal/campaigns/retry-stuck-now")
      .set(getAuthHeaders())
      .send({ all: true });

    expect(res.status).toBe(200);
    expect(res.body.cancelled).toBe(0);
    expect(res.body.stillSending).toBe(1);

    const [row] = await db.select().from(instantlyCampaigns).where(eq(instantlyCampaigns.instantlyCampaignId, "inst-healthy"));
    expect(row.deliveryStatus).toBe("contacted");
    expect(row.status).toBe("active");
  });

  it("is idempotent — re-running yields no additional cancels", async () => {
    await db.insert(instantlyCampaigns).values({
      campaignId: "camp-1",
      orgId: "org-1",
      userId: "00000000-0000-0000-0000-000000000001",
      brandIds: ["brand-1"],
      status: "active",
      deliveryStatus: "contacted",
      name: "test",
      leadEmail: "stuck@test.com",
      instantlyCampaignId: "inst-stuck",
    });

    vi.mocked(getCampaign).mockResolvedValue({ not_sending_status: "x" } as any);

    const first = await request(app)
      .post("/internal/campaigns/retry-stuck-now")
      .set(getAuthHeaders())
      .send({ all: true });
    expect(first.body.cancelled).toBe(1);

    const second = await request(app)
      .post("/internal/campaigns/retry-stuck-now")
      .set(getAuthHeaders())
      .send({ all: true });
    // Once the row's deliveryStatus is 'cancelled', it falls outside the SELECT.
    expect(second.body.cancelled).toBe(0);
    expect(second.body.scanned).toBe(0);
  });
});
