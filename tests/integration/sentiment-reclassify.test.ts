/**
 * DB-backed proof that gold-layer reply-sentiment stats reflect a lead's
 * CURRENT sentiment (latest event, manual winning ties) — NOT every sentiment
 * event ever recorded.
 *
 * Bug: a reply auto-classified `lead_interested` then manually re-qualified
 * `lead_not_interested` (POST /orgs/manual-qualifications) used to keep counting
 * as a positive reply at model/brand level, because the gold queries counted
 * raw events. The silver event log keeps BOTH rows (append-only); gold must read
 * only the latest. See queryGroupedStats / SENTIMENT_EVENT_TYPES in analytics.ts.
 *
 * Skipped when no DB URL is configured (e.g. CI runs unit tests only).
 */
import { describe, it, expect, beforeEach, afterAll } from "vitest";
import { db } from "../../src/db";
import { instantlyCampaigns, instantlyEvents } from "../../src/db/schema";
import { sql } from "drizzle-orm";
import { cleanTestData, closeDb } from "../helpers/test-db";
import { queryStats, queryGroupedStats, queryStepSentiment } from "../../src/routes/analytics";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;

const ORG = "org-sentiment";
const BASE = {
  campaignId: "camp-s",
  orgId: ORG,
  userId: "00000000-0000-0000-0000-000000000001",
  brandIds: ["brand-s"],
  workflowSlug: "wf-s",
  status: "active",
  deliveryStatus: "replied",
  name: "test",
};

describe.skipIf(SKIP)("reply-sentiment reclassification (DB-backed)", () => {
  beforeEach(async () => {
    await cleanTestData();
  });

  afterAll(async () => {
    await cleanTestData();
    await closeDb();
  });

  async function seedReclassifiedLead() {
    // One per-lead campaign (1 campaign = 1 lead).
    await db.insert(instantlyCampaigns).values({
      ...BASE,
      leadEmail: "prospect@external.com",
      instantlyCampaignId: "inst-s",
    });
    // Earlier: auto lead_interested. Later: manual lead_not_interested.
    await db.insert(instantlyEvents).values([
      {
        eventType: "lead_interested",
        campaignId: "inst-s",
        leadEmail: "prospect@external.com",
        timestamp: new Date("2026-06-01T10:00:00Z"),
        source: "webhook",
      },
      {
        eventType: "lead_not_interested",
        campaignId: "inst-s",
        leadEmail: "prospect@external.com",
        timestamp: new Date("2026-06-02T10:00:00Z"),
        source: "manual",
      },
    ]);
  }

  const whereOrg = sql`c.org_id = ${ORG}`;

  it("aggregate /stats counts the lead as the CURRENT (negative) sentiment, not the stale positive", async () => {
    await seedReclassifiedLead();

    const { recipientStats } = await queryStats(whereOrg);

    expect(recipientStats.repliesPositive).toBe(0);
    expect(recipientStats.repliesNegative).toBe(1);
    expect(recipientStats.repliesDetail.interested).toBe(0);
    expect(recipientStats.repliesDetail.notInterested).toBe(1);
  });

  it("grouped (model/brand) stats also reflect the current sentiment", async () => {
    await seedReclassifiedLead();

    const byWorkflow = await queryGroupedStats(whereOrg, "workflowSlug");
    const wf = byWorkflow.find((g) => g.key === "wf-s");
    expect(wf?.recipientStats.repliesPositive).toBe(0);
    expect(wf?.recipientStats.repliesNegative).toBe(1);

    const byBrand = await queryGroupedStats(whereOrg, "brandId");
    const brand = byBrand.find((g) => g.key === "brand-s");
    expect(brand?.recipientStats.repliesPositive).toBe(0);
    expect(brand?.recipientStats.repliesNegative).toBe(1);
  });

  it("manual wins a same-timestamp tie over the auto event", async () => {
    await db.insert(instantlyCampaigns).values({
      ...BASE,
      leadEmail: "tie@external.com",
      instantlyCampaignId: "inst-tie",
    });
    const sameTs = new Date("2026-06-03T10:00:00Z");
    await db.insert(instantlyEvents).values([
      { eventType: "lead_interested", campaignId: "inst-tie", leadEmail: "tie@external.com", timestamp: sameTs, source: "webhook" },
      { eventType: "lead_not_interested", campaignId: "inst-tie", leadEmail: "tie@external.com", timestamp: sameTs, source: "manual" },
    ]);

    const { recipientStats } = await queryStats(whereOrg);
    expect(recipientStats.repliesPositive).toBe(0);
    expect(recipientStats.repliesNegative).toBe(1);
  });

  it("per-step: a re-qualified negative lands on the LAST step, never positive on an earlier one", async () => {
    await db.insert(instantlyCampaigns).values({
      ...BASE,
      leadEmail: "step@external.com",
      instantlyCampaignId: "inst-step",
    });
    await db.insert(instantlyEvents).values([
      // 2 emails sent: steps 1 and 2
      { eventType: "email_sent", campaignId: "inst-step", leadEmail: "step@external.com", step: 1, timestamp: new Date("2026-06-01T09:00:00Z"), source: "webhook" },
      { eventType: "email_sent", campaignId: "inst-step", leadEmail: "step@external.com", step: 2, timestamp: new Date("2026-06-02T09:00:00Z"), source: "webhook" },
      // auto interested at step 2, then manual not_interested
      { eventType: "lead_interested", campaignId: "inst-step", leadEmail: "step@external.com", step: 2, timestamp: new Date("2026-06-02T10:00:00Z"), source: "webhook" },
      { eventType: "lead_not_interested", campaignId: "inst-step", leadEmail: "step@external.com", step: null, timestamp: new Date("2026-06-03T10:00:00Z"), source: "manual" },
    ]);

    const byStep = await queryStepSentiment(whereOrg);
    // Current sentiment (negative) attributed to the last sent step (2), nowhere positive.
    expect(byStep.get(2)?.notInterested).toBe(1);
    expect(byStep.get(2)?.interested).toBe(0);
    expect(byStep.get(1)?.interested ?? 0).toBe(0);
    expect(byStep.get(1)?.notInterested ?? 0).toBe(0);
  });

  it("a still-positive reply (no reclassification) stays positive", async () => {
    await db.insert(instantlyCampaigns).values({
      ...BASE,
      leadEmail: "happy@external.com",
      instantlyCampaignId: "inst-happy",
    });
    await db.insert(instantlyEvents).values({
      eventType: "lead_interested",
      campaignId: "inst-happy",
      leadEmail: "happy@external.com",
      timestamp: new Date("2026-06-01T10:00:00Z"),
      source: "webhook",
    });

    const { recipientStats } = await queryStats(whereOrg);
    expect(recipientStats.repliesPositive).toBe(1);
    expect(recipientStats.repliesNegative).toBe(0);
  });
});
