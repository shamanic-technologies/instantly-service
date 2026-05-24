/**
 * End-to-end integration test for the retry-stuck heartbeat sweep.
 *
 * Seeds real fixture rows in the local test DB (`instantly_campaigns`,
 * `instantly_leads`, `sequence_costs`, `instantly_events`), stubs the
 * Instantly + runs-service + send-lead boundaries, calls `runRetryStuck()`
 * directly, and asserts the row lifecycle: re-sent (row mutated + costs
 * rotated) or left alone (no terminal cancel). Skipped when no DB URL is
 * configured.
 */
import { describe, it, expect, vi, beforeEach, afterAll } from "vitest";
import { eq } from "drizzle-orm";

// Mock the outbound HTTP clients BEFORE importing the module under test.
// The DB itself is real so SQL writes observe the fixture rows.
vi.mock("../../src/lib/instantly-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/lib/instantly-client")>();
  return {
    ...actual,
    getCampaign: vi.fn(),
    updateCampaignStatus: vi.fn(),
  };
});

// Send path goes through send-lead.ts → real listAccounts/createCampaign
// would hit Instantly's API. Mock the helper to return a deterministic
// outcome without touching the network.
vi.mock("../../src/lib/send-lead", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/lib/send-lead")>();
  return {
    ...actual,
    sendLeadToInstantly: vi.fn(),
  };
});

vi.mock("../../src/lib/runs-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/lib/runs-client")>();
  return {
    ...actual,
    createRun: vi.fn(),
    updateRun: vi.fn().mockResolvedValue({}),
    addCosts: vi.fn(),
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
import { instantlyCampaigns, instantlyLeads, sequenceCosts, instantlyEvents } from "../../src/db/schema";
import { getCampaign } from "../../src/lib/instantly-client";
import { sendLeadToInstantly } from "../../src/lib/send-lead";
import { createRun, addCosts } from "../../src/lib/runs-client";
import { runRetryStuck } from "../../src/lib/retry-stuck";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;

// Rows must be older than 72h to fall inside the SELECT's age filter.
const STUCK_CREATED_AT = new Date(Date.now() - 75 * 60 * 60 * 1000);

describe.skipIf(SKIP)("runRetryStuck (DB-backed)", () => {
  beforeEach(async () => {
    await cleanTestData();
    vi.clearAllMocks();
  });

  afterAll(async () => {
    await cleanTestData();
    await closeDb();
  });

  it("re-sends every stuck fixture row onto a fresh account when a sender is available", async () => {
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

    await db.insert(instantlyLeads).values([
      { instantlyCampaignId: "inst-a", email: "stuck-a@test.com", firstName: "A", orgId: "org-1" },
      { instantlyCampaignId: "inst-b", email: "stuck-b@test.com", firstName: "B", orgId: "org-1" },
    ]);

    await db.insert(sequenceCosts).values([
      { campaignId: "camp-1", leadEmail: "stuck-a@test.com", step: 1, runId: "run-a-1", costId: "cost-a-1", status: "provisioned" },
      { campaignId: "camp-1", leadEmail: "stuck-b@test.com", step: 1, runId: "run-b-1", costId: "cost-b-1", status: "provisioned" },
    ]);

    // Live Instantly campaign has a 1-step sequence so re-send can rebuild it.
    vi.mocked(getCampaign).mockResolvedValue({
      sequences: [{ steps: [{ delay: 0, variants: [{ subject: "Hi", body: "Body" }] }] }],
    } as any);

    // Pretend each row got sent to a different new Instantly campaign.
    let counter = 0;
    vi.mocked(sendLeadToInstantly).mockImplementation(async () => {
      counter += 1;
      return {
        ok: true,
        value: {
          instantlyCampaignId: `inst-NEW-${counter}`,
          added: 1,
          account: { email: `sender-${counter}@new.test`, status: 1, warmup_status: 1 } as any,
        },
      };
    });

    vi.mocked(createRun).mockImplementation(async () => ({ id: `new-step-run-${Math.random()}` }) as any);
    vi.mocked(addCosts).mockResolvedValue({
      costs: [
        { id: "new-cost-account", costName: "instantly-account-email-sent" },
        { id: "new-cost-domain", costName: "instantly-domain-email-sent" },
      ],
    } as any);

    const summary = await runRetryStuck();
    expect(summary.redispatched).toBe(2);
    expect(summary.failed).toBe(0);
    expect(summary.scanned).toBe(2);
    expect(summary.skipped).toBeUndefined();

    // Rows now point at the new Instantly campaigns; delivery_status stays 'contacted'.
    const rows = await db.select().from(instantlyCampaigns);
    expect(rows).toHaveLength(2);
    for (const r of rows) {
      expect(r.deliveryStatus).toBe("contacted");
      expect(r.status).toBe("active");
      expect(r.instantlyCampaignId).toMatch(/^inst-NEW-\d$/);
      expect((r.metadata as Record<string, unknown>).redispatchCount).toBe(1);
      const history = (r.metadata as { redispatchHistory: Array<Record<string, unknown>> }).redispatchHistory;
      expect(history).toHaveLength(1);
      expect(history[0].from).toMatch(/^inst-[ab]$/);
    }

    // Old cost rows are now `cancelled` (refund). New `provisioned` rows are present.
    const allCosts = await db.select().from(sequenceCosts);
    const cancelled = allCosts.filter((c) => c.status === "cancelled");
    const provisioned = allCosts.filter((c) => c.status === "provisioned");
    expect(cancelled).toHaveLength(2); // original cost-a-1, cost-b-1
    expect(provisioned.length).toBeGreaterThanOrEqual(2); // new costs for each re-send
  });

  it("leaves the row alone (no terminal cancel) when no sender is available", async () => {
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

    await db.insert(instantlyLeads).values({
      instantlyCampaignId: "inst-stuck",
      email: "stuck@test.com",
      orgId: "org-1",
    });

    await db.insert(sequenceCosts).values({
      campaignId: "camp-1",
      leadEmail: "stuck@test.com",
      step: 1,
      runId: "run-stuck-1",
      costId: "cost-stuck-1",
      status: "provisioned",
    });

    vi.mocked(getCampaign).mockResolvedValue({
      sequences: [{ steps: [{ delay: 0, variants: [{ subject: "Hi", body: "Body" }] }] }],
    } as any);

    vi.mocked(sendLeadToInstantly).mockResolvedValue({
      ok: false,
      reason: "no_healthy_account",
    });

    const summary = await runRetryStuck();
    expect(summary.failed).toBe(1);
    expect(summary.redispatched).toBe(0);

    const [row] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.instantlyCampaignId, "inst-stuck"));
    // No terminal cancel — row stays alive for the next tick to retry.
    expect(row.deliveryStatus).toBe("contacted");
    expect(row.status).toBe("active");

    // Costs are untouched (no cancel/provision when the send fails).
    const [cost] = await db
      .select()
      .from(sequenceCosts)
      .where(eq(sequenceCosts.costId, "cost-stuck-1"));
    expect(cost.status).toBe("provisioned");
  });

  it("ignores rows that already have a silver email_sent event", async () => {
    await db.insert(instantlyCampaigns).values({
      campaignId: "camp-1",
      orgId: "org-1",
      userId: "00000000-0000-0000-0000-000000000001",
      brandIds: ["brand-1"],
      status: "active",
      deliveryStatus: "contacted",
      name: "test",
      leadEmail: "sent@test.com",
      instantlyCampaignId: "inst-sent",
      createdAt: STUCK_CREATED_AT,
    });

    // Silver says email_sent fired even though delivery_status stayed
    // 'contacted' (rare promotion miss). retry-stuck must skip.
    await db.insert(instantlyEvents).values({
      eventType: "email_sent",
      campaignId: "inst-sent",
      leadEmail: "sent@test.com",
      accountEmail: "sender@test.com",
      step: 1,
      variant: 1,
      timestamp: new Date(),
      source: "webhook",
    });

    vi.mocked(sendLeadToInstantly).mockResolvedValue({
      ok: true,
      value: {
        instantlyCampaignId: "inst-NEW",
        added: 1,
        account: { email: "should-not-be-used@test.com", status: 1, warmup_status: 1 } as any,
      },
    });

    const summary = await runRetryStuck();
    expect(summary.scanned).toBe(0);
    expect(summary.redispatched).toBe(0);
    expect(sendLeadToInstantly).not.toHaveBeenCalled();

    const [row] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.instantlyCampaignId, "inst-sent"));
    expect(row.instantlyCampaignId).toBe("inst-sent");
  });

  it("ignores rows younger than the 72h age floor", async () => {
    // Row created 25h ago — was previously picked up at the 24h floor, must
    // NOT be picked up at the new 72h floor.
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
      createdAt: new Date(Date.now() - 25 * 60 * 60 * 1000),
    });

    const summary = await runRetryStuck();
    expect(summary.scanned).toBe(0);

    const [row] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.instantlyCampaignId, "inst-young"));
    expect(row.deliveryStatus).toBe("contacted");
  });
});
