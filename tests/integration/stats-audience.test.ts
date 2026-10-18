/**
 * CAMPAIGN-STATS-SENT-ZERO-1018 — DB-backed proof that WHO a stats read is for
 * decides what it leaves out.
 *
 * Measured 2026-10-18: a staff test org's campaign was billed 13 sends and its
 * leads list showed them, while `GET /orgs/stats` read `sent 0`: the read
 * applied a hard-coded internal-lead address list (distribute.you, ...) and
 * every sent lead of that campaign was on @distribute.you.
 *
 * - An ORG read counts every lead in the org's scope, whatever its address.
 * - A FLEET read (`/public/stats`) keeps staff test activity out.
 * - Both drop a self-addressed send (lead = the sending mailbox).
 *
 * Skipped when no DB URL is configured (CI runs unit tests only).
 */
import { describe, it, expect, beforeEach, afterAll } from "vitest";
import { db } from "../../src/db";
import { instantlyCampaigns, instantlyEvents } from "../../src/db/schema";
import { sql } from "drizzle-orm";
import { cleanTestData, closeDb } from "../helpers/test-db";
import { queryStats, queryGroupedStats, computeStatsPayload } from "../../src/routes/analytics";

const SKIP = !process.env.INSTANTLY_SERVICE_DATABASE_URL;

const ORG = "org-stats-audience";
const BASE = {
  campaignId: "camp-audience",
  orgId: ORG,
  userId: "00000000-0000-0000-0000-000000000001",
  brandIds: ["brand-audience"],
  workflowSlug: "wf-audience",
  status: "active",
  deliveryStatus: "replied",
  name: "test",
};

describe.skipIf(SKIP)("stats audience: org read vs fleet read (DB-backed)", () => {
  beforeEach(async () => {
    await cleanTestData();
  });

  afterAll(async () => {
    await cleanTestData();
    await closeDb();
  });

  async function seed() {
    await db.insert(instantlyCampaigns).values([
      // A staff test lead on @distribute.you, sent + replied interested.
      { ...BASE, leadEmail: "tester@distribute.you", instantlyCampaignId: "inst-staff" },
      // A sequence addressed to its own sending mailbox.
      { ...BASE, leadEmail: "kevin@boostdistribute.com", instantlyCampaignId: "inst-self", deliveryStatus: "sent" },
    ]);
    await db.insert(instantlyEvents).values([
      {
        eventType: "email_sent",
        campaignId: "inst-staff",
        leadEmail: "tester@distribute.you",
        accountEmail: "kevin@boostdistribute.com",
        step: 1,
        timestamp: new Date("2026-10-14T10:00:00Z"),
        source: "self_send",
      },
      {
        eventType: "reply_received",
        campaignId: "inst-staff",
        leadEmail: "tester@distribute.you",
        accountEmail: "kevin@boostdistribute.com",
        step: 1,
        timestamp: new Date("2026-10-14T11:00:00Z"),
        source: "webhook",
      },
      {
        eventType: "lead_interested",
        campaignId: "inst-staff",
        leadEmail: "tester@distribute.you",
        timestamp: new Date("2026-10-14T11:00:01Z"),
        source: "webhook",
      },
      {
        eventType: "email_sent",
        campaignId: "inst-self",
        leadEmail: "kevin@boostdistribute.com",
        accountEmail: "kevin@boostdistribute.com",
        step: 1,
        timestamp: new Date("2026-10-14T10:00:00Z"),
        source: "self_send",
      },
    ]);
  }

  const whereOrg = sql`c.org_id = ${ORG}`;

  it("org read counts the org's @distribute.you lead: sent 1, positive reply counted", async () => {
    await seed();
    const { recipientStats, emailStats } = await queryStats(whereOrg, "org");
    expect(recipientStats.contacted).toBe(2);
    // The self-addressed send reached no person: not counted.
    expect(recipientStats.sent).toBe(1);
    expect(emailStats.sent).toBe(1);
    expect(recipientStats.repliesPositive).toBe(1);
    expect(recipientStats.repliesDetail.interested).toBe(1);

    const payload = await computeStatsPayload(whereOrg, "org");
    expect((payload.emailStats as any).stepStats[0].sent).toBe(1);

    const byCampaign = await queryGroupedStats(whereOrg, "org", "campaignId");
    expect(byCampaign).toHaveLength(1);
    expect(byCampaign[0].recipientStats.sent).toBe(1);
    expect(byCampaign[0].recipientStats.repliesPositive).toBe(1);
  });

  it("fleet read still keeps staff test activity out", async () => {
    await seed();
    const { recipientStats, emailStats } = await queryStats(whereOrg, "fleet");
    // The @distribute.you lead is gone from every figure; the self-send lead is
    // not on the list, so its campaign row is still contacted, its send is not.
    expect(recipientStats.contacted).toBe(1);
    expect(recipientStats.sent).toBe(0);
    expect(emailStats.sent).toBe(0);
    expect(recipientStats.repliesPositive).toBe(0);

    const byCampaign = await queryGroupedStats(whereOrg, "fleet", "campaignId");
    expect(byCampaign.every((g) => g.recipientStats.sent === 0 && g.recipientStats.repliesPositive === 0)).toBe(true);
  });
});
