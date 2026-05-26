/**
 * End-to-end integration test for the simplified retry-stuck primitives.
 *
 * Seeds real fixture rows in the local test DB (`instantly_campaigns`,
 * `instantly_leads`, `sequence_costs`, `instantly_events`), stubs the
 * Instantly + runs-service + send-lead boundaries, and exercises
 * `selectOneStuckRow` + `processRow` directly. Skipped when no DB URL is
 * configured.
 */
import { describe, it, expect, vi, beforeEach, afterAll } from "vitest";
import { eq } from "drizzle-orm";

vi.mock("../../src/lib/instantly-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/lib/instantly-client")>();
  return {
    ...actual,
    getCampaign: vi.fn(),
    updateCampaignStatus: vi.fn(),
  };
});

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
    getRun: vi.fn(),
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
import { createRun, addCosts, getRun } from "../../src/lib/runs-client";
import { selectOneStuckRow, processRow } from "../../src/lib/retry-stuck";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;
const STUCK_CREATED_AT = new Date(Date.now() - 75 * 60 * 60 * 1000);

describe.skipIf(SKIP)("retry-stuck (DB-backed)", () => {
  beforeEach(async () => {
    await cleanTestData();
    vi.clearAllMocks();
  });

  afterAll(async () => {
    await cleanTestData();
    await closeDb();
  });

  it("selectOneStuckRow picks the oldest matching row", async () => {
    const base = {
      campaignId: "camp-1",
      orgId: "org-1",
      userId: "00000000-0000-0000-0000-000000000001",
      brandIds: ["brand-1"],
      status: "active",
      deliveryStatus: "contacted",
      name: "test",
    };

    await db.insert(instantlyCampaigns).values([
      {
        ...base,
        leadEmail: "a@test.com",
        instantlyCampaignId: "inst-a",
        createdAt: new Date(Date.now() - 100 * 60 * 60 * 1000),
      },
      {
        ...base,
        leadEmail: "b@test.com",
        instantlyCampaignId: "inst-b",
        createdAt: new Date(Date.now() - 80 * 60 * 60 * 1000),
      },
    ]);

    const r = await selectOneStuckRow();
    expect(r).not.toBeNull();
    expect(r!.instantlyCampaignId).toBe("inst-a");
  });

  it("selectOneStuckRow returns null when backlog is empty", async () => {
    const r = await selectOneStuckRow();
    expect(r).toBeNull();
  });

  it("selectOneStuckRow ignores rows younger than 72h", async () => {
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

    const r = await selectOneStuckRow();
    expect(r).toBeNull();
  });

  it("selectOneStuckRow ignores rows that already have a silver email_sent event", async () => {
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

    const r = await selectOneStuckRow();
    expect(r).toBeNull();
  });

  it("processRow re-sends the lead on a fresh account and updates the row in place", async () => {
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
      firstName: "Stuck",
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
      ok: true,
      value: {
        instantlyCampaignId: "inst-NEW",
        added: 1,
        account: { email: "sender@new.test", status: 1, warmup_status: 1 } as any,
      },
    });

    vi.mocked(getRun).mockResolvedValue({
      id: "run-stuck-1",
      organizationId: "org-1",
      userId: "00000000-0000-0000-0000-000000000001",
      parentRunId: null,
    } as any);
    vi.mocked(createRun).mockImplementation(async () => ({ id: `step-run-${Math.random()}` }) as any);
    vi.mocked(addCosts).mockResolvedValue({
      costs: [
        { id: "new-cost-account", costName: "instantly-account-email-sent" },
        { id: "new-cost-domain", costName: "instantly-domain-email-sent" },
      ],
    } as any);

    const candidate = await selectOneStuckRow();
    expect(candidate).not.toBeNull();

    const outcome = await processRow(candidate!);
    expect(outcome.kind).toBe("redispatched");

    const [row] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.id, candidate!.id));
    expect(row.deliveryStatus).toBe("contacted");
    expect(row.status).toBe("active");
    expect(row.instantlyCampaignId).toBe("inst-NEW");
    expect((row.metadata as Record<string, unknown>).redispatchCount).toBe(1);
  });

  it("processRow leaves the row alone (no terminal cancel) when no sender is available", async () => {
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

    vi.mocked(getRun).mockResolvedValue({
      id: "run-stuck-1",
      organizationId: "org-1",
      userId: "00000000-0000-0000-0000-000000000001",
      parentRunId: null,
    } as any);

    vi.mocked(getCampaign).mockResolvedValue({
      sequences: [{ steps: [{ delay: 0, variants: [{ subject: "Hi", body: "Body" }] }] }],
    } as any);

    vi.mocked(sendLeadToInstantly).mockResolvedValue({
      ok: false,
      reason: "no_healthy_accounts_available",
    });

    const candidate = await selectOneStuckRow();
    expect(candidate).not.toBeNull();

    const outcome = await processRow(candidate!);
    expect(outcome).toEqual({ kind: "failed", reason: "no_healthy_accounts_available" });

    const [row] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.instantlyCampaignId, "inst-stuck"));
    // No terminal cancel — row stays alive for the next loop iteration.
    expect(row.deliveryStatus).toBe("contacted");
    expect(row.status).toBe("active");

    // Costs untouched.
    const [cost] = await db
      .select()
      .from(sequenceCosts)
      .where(eq(sequenceCosts.costId, "cost-stuck-1"));
    expect(cost.status).toBe("provisioned");
  });
});
