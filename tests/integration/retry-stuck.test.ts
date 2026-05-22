/**
 * End-to-end integration test for the retry-stuck sweep.
 *
 * Seeds real fixture rows in the local test DB (`instantly_campaigns`,
 * `sequence_costs`), stubs the Instantly + runs-service HTTP boundaries,
 * calls `runRetryStuck()` directly (the production cron path), and asserts
 * the rows flip to `delivery_status='cancelled'` while sequence costs go
 * to `status='cancelled'` AND the `not_sending_status` / `_seen_at`
 * columns are populated. Skipped when no DB URL is configured.
 */
import { describe, it, expect, vi, beforeEach, afterAll } from "vitest";
import { eq } from "drizzle-orm";

// Mock the outbound HTTP clients BEFORE importing the module under test.
// The DB itself is real so the SQL UPDATE in handleCampaignError + retry-stuck
// observe the fixture rows.
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

import { cleanTestData, closeDb } from "../helpers/test-db";
import { db } from "../../src/db";
import { instantlyCampaigns, sequenceCosts } from "../../src/db/schema";
import { getCampaign } from "../../src/lib/instantly-client";
import { runRetryStuck } from "../../src/lib/retry-stuck";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;

// Rows must be older than 24h to fall inside the SELECT's age filter.
const STUCK_CREATED_AT = new Date(Date.now() - 25 * 60 * 60 * 1000);

describe.skipIf(SKIP)("runRetryStuck (DB-backed)", () => {
  beforeEach(async () => {
    await cleanTestData();
    vi.clearAllMocks();
  });

  afterAll(async () => {
    await cleanTestData();
    await closeDb();
  });

  it("cancels every stuck fixture row and populates not_sending_status columns", async () => {
    const baseCampaign = {
      campaignId: "camp-1",
      orgId: "org-1",
      userId: "00000000-0000-0000-0000-000000000001",
      brandIds: ["brand-1"],
      status: "active",
      deliveryStatus: "contacted",
      name: "test",
      createdAt: STUCK_CREATED_AT,
    };

    await db.insert(instantlyCampaigns).values([
      { ...baseCampaign, leadEmail: "stuck-a@test.com", instantlyCampaignId: "inst-a" },
      { ...baseCampaign, leadEmail: "stuck-b@test.com", instantlyCampaignId: "inst-b" },
    ]);

    await db.insert(sequenceCosts).values([
      { campaignId: "camp-1", leadEmail: "stuck-a@test.com", step: 1, runId: "run-a-1", costId: "cost-a-1", status: "provisioned" },
      { campaignId: "camp-1", leadEmail: "stuck-b@test.com", step: 1, runId: "run-b-1", costId: "cost-b-1", status: "provisioned" },
    ]);

    vi.mocked(getCampaign).mockResolvedValue({ not_sending_status: 4 } as any);

    const summary = await runRetryStuck();
    expect(summary.cancelled).toBe(2);
    expect(summary.scanned).toBe(2);
    expect(summary.skipped).toBeUndefined();

    const [rowA] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.instantlyCampaignId, "inst-a"));
    const [rowB] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.instantlyCampaignId, "inst-b"));
    expect(rowA.deliveryStatus).toBe("cancelled");
    expect(rowB.deliveryStatus).toBe("cancelled");
    expect(rowA.status).toBe("error");
    expect((rowA.metadata as Record<string, unknown>).retryCount).toBe(1);

    // The diagnostic columns added by PR A are populated by this sweep.
    expect(rowA.notSendingStatus).toBe(4);
    expect(rowB.notSendingStatus).toBe(4);
    expect(rowA.notSendingStatusSeenAt).toBeInstanceOf(Date);
    expect(rowB.notSendingStatusSeenAt).toBeInstanceOf(Date);

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
      createdAt: STUCK_CREATED_AT,
    });

    vi.mocked(getCampaign).mockResolvedValue({ not_sending_status: null } as any);

    const summary = await runRetryStuck();
    expect(summary.cancelled).toBe(0);
    expect(summary.stillSending).toBe(1);

    const [row] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.instantlyCampaignId, "inst-healthy"));
    expect(row.deliveryStatus).toBe("contacted");
    expect(row.status).toBe("active");
    // No diagnostic write when the row is still sending.
    expect(row.notSendingStatus).toBeNull();
    expect(row.notSendingStatusSeenAt).toBeNull();
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
      createdAt: STUCK_CREATED_AT,
    });

    vi.mocked(getCampaign).mockResolvedValue({ not_sending_status: 2 } as any);

    const first = await runRetryStuck();
    expect(first.cancelled).toBe(1);

    const second = await runRetryStuck();
    // Once the row's deliveryStatus is 'cancelled', it falls outside the SELECT.
    expect(second.cancelled).toBe(0);
    expect(second.scanned).toBe(0);
  });

  it("ignores rows younger than the 24h age floor", async () => {
    // Row created 1h ago — should NOT be picked up by the cron sweep.
    await db.insert(instantlyCampaigns).values({
      campaignId: "camp-1",
      orgId: "org-1",
      userId: "00000000-0000-0000-0000-000000000001",
      brandIds: ["brand-1"],
      status: "active",
      deliveryStatus: "contacted",
      name: "test",
      leadEmail: "young@test.com",
      instantlyCampaignId: "inst-young",
      createdAt: new Date(Date.now() - 60 * 60 * 1000),
    });

    vi.mocked(getCampaign).mockResolvedValue({ not_sending_status: 4 } as any);

    const summary = await runRetryStuck();
    expect(summary.scanned).toBe(0);

    const [row] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.instantlyCampaignId, "inst-young"));
    expect(row.deliveryStatus).toBe("contacted");
  });
});
