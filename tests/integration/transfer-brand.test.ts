import { describe, it, expect, beforeEach, afterAll } from "vitest";
import request from "supertest";
import { createTestApp, getAuthHeaders } from "../helpers/test-app";
import { cleanTestData, closeDb, randomUUID } from "../helpers/test-db";
import { db } from "../../src/db";
import { instantlyCampaigns } from "../../src/db/schema";
import { eq } from "drizzle-orm";

const app = createTestApp();
const headers = getAuthHeaders();

const SOURCE_ORG = "source-org-" + randomUUID();
const TARGET_ORG = "target-org-" + randomUUID();
const BRAND_ID = "brand-" + randomUUID();
const OTHER_BRAND = "other-brand-" + randomUUID();

beforeEach(async () => {
  await cleanTestData();
});

afterAll(async () => {
  await cleanTestData();
  await closeDb();
});

describe("POST /internal/transfer-brand", () => {
  it("returns 401 without api key", async () => {
    const res = await request(app)
      .post("/internal/transfer-brand")
      .send({ brandId: BRAND_ID, sourceOrgId: SOURCE_ORG, targetOrgId: TARGET_ORG });

    expect(res.status).toBe(401);
  });

  it("returns 400 for invalid body", async () => {
    const res = await request(app)
      .post("/internal/transfer-brand")
      .set(headers)
      .send({ brandId: BRAND_ID });

    expect(res.status).toBe(400);
  });

  it("transfers solo-brand campaign rows", async () => {
    const campaignId = randomUUID();
    await db.insert(instantlyCampaigns).values({
      instantlyCampaignId: "ic-" + randomUUID(),
      name: "Solo brand campaign",
      orgId: SOURCE_ORG,
      brandIds: [BRAND_ID],
      campaignId,
    });

    const res = await request(app)
      .post("/internal/transfer-brand")
      .set(headers)
      .send({ brandId: BRAND_ID, sourceOrgId: SOURCE_ORG, targetOrgId: TARGET_ORG });

    expect(res.status).toBe(200);
    expect(res.body.updatedTables).toEqual([
      { tableName: "instantly_campaigns", count: 1 },
    ]);

    const [row] = await db.select().from(instantlyCampaigns).where(eq(instantlyCampaigns.campaignId, campaignId));
    expect(row.orgId).toBe(TARGET_ORG);
  });

  it("skips co-branding rows (multiple brand IDs)", async () => {
    const campaignId = randomUUID();
    await db.insert(instantlyCampaigns).values({
      instantlyCampaignId: "ic-" + randomUUID(),
      name: "Co-brand campaign",
      orgId: SOURCE_ORG,
      brandIds: [BRAND_ID, OTHER_BRAND],
      campaignId,
    });

    const res = await request(app)
      .post("/internal/transfer-brand")
      .set(headers)
      .send({ brandId: BRAND_ID, sourceOrgId: SOURCE_ORG, targetOrgId: TARGET_ORG });

    expect(res.status).toBe(200);
    expect(res.body.updatedTables).toEqual([
      { tableName: "instantly_campaigns", count: 0 },
    ]);

    const [row] = await db.select().from(instantlyCampaigns).where(eq(instantlyCampaigns.campaignId, campaignId));
    expect(row.orgId).toBe(SOURCE_ORG);
  });

  it("skips rows belonging to a different org", async () => {
    const campaignId = randomUUID();
    await db.insert(instantlyCampaigns).values({
      instantlyCampaignId: "ic-" + randomUUID(),
      name: "Other org campaign",
      orgId: "unrelated-org",
      brandIds: [BRAND_ID],
      campaignId,
    });

    const res = await request(app)
      .post("/internal/transfer-brand")
      .set(headers)
      .send({ brandId: BRAND_ID, sourceOrgId: SOURCE_ORG, targetOrgId: TARGET_ORG });

    expect(res.status).toBe(200);
    expect(res.body.updatedTables).toEqual([
      { tableName: "instantly_campaigns", count: 0 },
    ]);

    const [row] = await db.select().from(instantlyCampaigns).where(eq(instantlyCampaigns.campaignId, campaignId));
    expect(row.orgId).toBe("unrelated-org");
  });

  it("is idempotent — second call is a no-op", async () => {
    await db.insert(instantlyCampaigns).values({
      instantlyCampaignId: "ic-" + randomUUID(),
      name: "Idempotent test",
      orgId: SOURCE_ORG,
      brandIds: [BRAND_ID],
      campaignId: randomUUID(),
    });

    await request(app)
      .post("/internal/transfer-brand")
      .set(headers)
      .send({ brandId: BRAND_ID, sourceOrgId: SOURCE_ORG, targetOrgId: TARGET_ORG });

    const res = await request(app)
      .post("/internal/transfer-brand")
      .set(headers)
      .send({ brandId: BRAND_ID, sourceOrgId: SOURCE_ORG, targetOrgId: TARGET_ORG });

    expect(res.status).toBe(200);
    expect(res.body.updatedTables).toEqual([
      { tableName: "instantly_campaigns", count: 0 },
    ]);
  });

  it("skips rows with a different solo brand", async () => {
    const campaignId = randomUUID();
    await db.insert(instantlyCampaigns).values({
      instantlyCampaignId: "ic-" + randomUUID(),
      name: "Different brand campaign",
      orgId: SOURCE_ORG,
      brandIds: [OTHER_BRAND],
      campaignId,
    });

    const res = await request(app)
      .post("/internal/transfer-brand")
      .set(headers)
      .send({ brandId: BRAND_ID, sourceOrgId: SOURCE_ORG, targetOrgId: TARGET_ORG });

    expect(res.status).toBe(200);
    expect(res.body.updatedTables).toEqual([
      { tableName: "instantly_campaigns", count: 0 },
    ]);

    const [row] = await db.select().from(instantlyCampaigns).where(eq(instantlyCampaigns.campaignId, campaignId));
    expect(row.orgId).toBe(SOURCE_ORG);
  });
});
