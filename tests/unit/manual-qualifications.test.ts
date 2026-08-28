import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

// ─── Mocks ───────────────────────────────────────────────────────────────────

// Tracks the lookup `select` for campaign verification.
const mockCampaignSelect = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: () => ({ where: mockCampaignSelect }) }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {
    campaignId: "campaign_id",
    instantlyCampaignId: "instantly_campaign_id",
    leadEmail: "lead_email",
    orgId: "org_id",
  },
}));

// Lib mocks — the route should orchestrate; lib internals are unit-tested
// implicitly via integration paths (or separately if exercised).
const mockInsertManualQualification = vi.fn();
const mockApplyManualQualificationSideEffects = vi.fn();
const mockListManualQualifications = vi.fn();
const mockWithdrawManualQualification = vi.fn();

vi.mock("../../src/lib/manual-qualifications", () => ({
  insertManualQualification: (...args: unknown[]) =>
    mockInsertManualQualification(...args),
  applyManualQualificationSideEffects: (...args: unknown[]) =>
    mockApplyManualQualificationSideEffects(...args),
  listManualQualifications: (...args: unknown[]) =>
    mockListManualQualifications(...args),
  withdrawManualQualification: (...args: unknown[]) =>
    mockWithdrawManualQualification(...args),
}));

async function createApp() {
  const router = (await import("../../src/routes/manual-qualifications")).default;
  const app = express();
  app.use(express.json());
  // Mimic requireOrgId: set res.locals.orgId/userId from headers if present.
  app.use((req, res, next) => {
    const orgId = req.headers["x-org-id"];
    if (typeof orgId === "string") res.locals.orgId = orgId;
    const userId = req.headers["x-user-id"];
    if (typeof userId === "string") res.locals.userId = userId;
    next();
  });
  app.use("/orgs/manual-qualifications", router);
  return app;
}

const SAMPLE_BRONZE_ROW = {
  id: "bronze-row-1",
  orgId: "org-1",
  campaignId: "camp-logical-1",
  instantlyCampaignId: "inst-camp-1",
  leadEmail: "lead@test.com",
  status: "lead_interested" as const,
  qualifiedBy: "user-1",
  notes: null,
  qualifiedAt: new Date("2026-05-24T10:00:00.000Z"),
};

beforeEach(() => {
  vi.resetAllMocks();
  mockCampaignSelect.mockResolvedValue([]);
  mockListManualQualifications.mockResolvedValue([]);
});

describe("POST /orgs/manual-qualifications", () => {
  it("rejects when x-user-id is missing", async () => {
    const app = await createApp();
    const res = await request(app)
      .post("/orgs/manual-qualifications")
      .set("x-org-id", "org-1")
      .send({
        campaign_id: "camp-logical-1",
        email: "lead@test.com",
        status: "lead_interested",
      });

    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/x-user-id/);
  });

  it("rejects invalid status enum", async () => {
    const app = await createApp();
    const res = await request(app)
      .post("/orgs/manual-qualifications")
      .set("x-org-id", "org-1")
      .set("x-user-id", "user-1")
      .send({
        campaign_id: "camp-logical-1",
        email: "lead@test.com",
        status: "not_a_real_status",
      });

    expect(res.status).toBe(400);
  });

  it("rejects when email is malformed", async () => {
    const app = await createApp();
    const res = await request(app)
      .post("/orgs/manual-qualifications")
      .set("x-org-id", "org-1")
      .set("x-user-id", "user-1")
      .send({
        campaign_id: "camp-logical-1",
        email: "not-an-email",
        status: "lead_interested",
      });

    expect(res.status).toBe(400);
  });

  it("returns 404 when (campaign_id, email) pair is not in the org", async () => {
    mockCampaignSelect.mockResolvedValue([]);
    const app = await createApp();
    const res = await request(app)
      .post("/orgs/manual-qualifications")
      .set("x-org-id", "org-1")
      .set("x-user-id", "user-1")
      .send({
        campaign_id: "camp-logical-1",
        email: "lead@test.com",
        status: "lead_interested",
      });

    expect(res.status).toBe(404);
    expect(res.body.error).toMatch(/not found/i);
  });

  it("inserts bronze + fires side effects on a new (campaign, lead, status) tuple", async () => {
    mockCampaignSelect.mockResolvedValue([
      { campaignId: "camp-logical-1", instantlyCampaignId: "inst-camp-1" },
    ]);
    mockInsertManualQualification.mockResolvedValue({
      inserted: true,
      row: SAMPLE_BRONZE_ROW,
    });

    const app = await createApp();
    const res = await request(app)
      .post("/orgs/manual-qualifications")
      .set("x-org-id", "org-1")
      .set("x-user-id", "user-1")
      .send({
        campaign_id: "camp-logical-1",
        email: "lead@test.com",
        status: "lead_interested",
        notes: "test note",
      });

    expect(res.status).toBe(200);
    expect(res.body.idempotent).toBe(false);
    expect(res.body.qualification.status).toBe("lead_interested");
    expect(res.body.qualification.email).toBe("lead@test.com");

    expect(mockInsertManualQualification).toHaveBeenCalledWith(
      expect.objectContaining({
        orgId: "org-1",
        campaignId: "camp-logical-1",
        instantlyCampaignId: "inst-camp-1",
        leadEmail: "lead@test.com",
        status: "lead_interested",
        qualifiedBy: "user-1",
        notes: "test note",
      }),
    );

    expect(mockApplyManualQualificationSideEffects).toHaveBeenCalledWith(
      expect.objectContaining({
        bronzeRowId: "bronze-row-1",
        instantlyCampaignId: "inst-camp-1",
        leadEmail: "lead@test.com",
        status: "lead_interested",
      }),
    );
  });

  it("is idempotent: same status as latest returns 200 without firing side effects", async () => {
    mockCampaignSelect.mockResolvedValue([
      { campaignId: "camp-logical-1", instantlyCampaignId: "inst-camp-1" },
    ]);
    mockInsertManualQualification.mockResolvedValue({
      inserted: false,
      row: SAMPLE_BRONZE_ROW,
    });

    const app = await createApp();
    const res = await request(app)
      .post("/orgs/manual-qualifications")
      .set("x-org-id", "org-1")
      .set("x-user-id", "user-1")
      .send({
        campaign_id: "camp-logical-1",
        email: "lead@test.com",
        status: "lead_interested",
      });

    expect(res.status).toBe(200);
    expect(res.body.idempotent).toBe(true);
    expect(res.body.qualification.id).toBe("bronze-row-1");
    expect(mockApplyManualQualificationSideEffects).not.toHaveBeenCalled();
  });
});

describe("GET /orgs/manual-qualifications", () => {
  it("returns org-scoped history", async () => {
    mockListManualQualifications.mockResolvedValue([
      SAMPLE_BRONZE_ROW,
      { ...SAMPLE_BRONZE_ROW, id: "bronze-row-2", status: "lead_neutral" as const },
    ]);

    const app = await createApp();
    const res = await request(app)
      .get("/orgs/manual-qualifications?campaign_id=camp-logical-1")
      .set("x-org-id", "org-1");

    expect(res.status).toBe(200);
    expect(res.body.qualifications).toHaveLength(2);
    expect(res.body.qualifications[0].id).toBe("bronze-row-1");
    expect(mockListManualQualifications).toHaveBeenCalledWith(
      expect.objectContaining({ orgId: "org-1", campaignId: "camp-logical-1" }),
    );
  });

  it("passes email filter through when provided", async () => {
    mockListManualQualifications.mockResolvedValue([]);
    const app = await createApp();
    const res = await request(app)
      .get(
        "/orgs/manual-qualifications?campaign_id=camp-logical-1&email=lead@test.com",
      )
      .set("x-org-id", "org-1");

    expect(res.status).toBe(200);
    expect(mockListManualQualifications).toHaveBeenCalledWith(
      expect.objectContaining({
        orgId: "org-1",
        campaignId: "camp-logical-1",
        leadEmail: "lead@test.com",
      }),
    );
  });

  it("scopes by orgId even when caller omits filters (cross-org leak prevention)", async () => {
    mockListManualQualifications.mockResolvedValue([]);
    const app = await createApp();
    await request(app).get("/orgs/manual-qualifications").set("x-org-id", "org-A");

    expect(mockListManualQualifications).toHaveBeenCalledWith(
      expect.objectContaining({ orgId: "org-A" }),
    );
    // No campaignId / leadEmail filters passed when caller omitted them.
    const call = mockListManualQualifications.mock.calls[0]?.[0] as Record<string, unknown>;
    expect(call.campaignId).toBeUndefined();
    expect(call.leadEmail).toBeUndefined();
  });
});

describe("POST /orgs/manual-qualifications/withdrawals", () => {
  const body = { campaign_id: "camp-logical-1", email: "lead@test.com" };

  it("rejects when x-user-id is missing — a withdrawal is somebody's act", async () => {
    const app = await createApp();
    const res = await request(app)
      .post("/orgs/manual-qualifications/withdrawals")
      .set("x-org-id", "org-1")
      .send(body);

    expect(res.status).toBe(400);
    expect(mockWithdrawManualQualification).not.toHaveBeenCalled();
  });

  it("404s with code campaign_not_found when the pair is not this org's", async () => {
    mockCampaignSelect.mockResolvedValue([]);
    const app = await createApp();
    const res = await request(app)
      .post("/orgs/manual-qualifications/withdrawals")
      .set("x-org-id", "org-1")
      .set("x-user-id", "user-1")
      .send(body);

    expect(res.status).toBe(404);
    expect(res.body.code).toBe("campaign_not_found");
    expect(mockWithdrawManualQualification).not.toHaveBeenCalled();
  });

  it("refuses a withdrawal on a pair nobody stated anything about — 404 with a code, distinguishable from a 500", async () => {
    mockCampaignSelect.mockResolvedValue([
      { campaignId: "camp-logical-1", instantlyCampaignId: "inst-camp-1" },
    ]);
    mockWithdrawManualQualification.mockResolvedValue({
      withdrawn: false,
      reason: "no_standing_qualification",
    });

    const app = await createApp();
    const res = await request(app)
      .post("/orgs/manual-qualifications/withdrawals")
      .set("x-org-id", "org-1")
      .set("x-user-id", "user-1")
      .send(body);

    expect(res.status).toBe(404);
    expect(res.body.code).toBe("no_standing_qualification");
    // Distinguishable from its sibling 404 by the code alone.
    expect(res.body.code).not.toBe("campaign_not_found");
  });

  it("withdraws the standing statement and returns it MARKED, not deleted", async () => {
    mockCampaignSelect.mockResolvedValue([
      { campaignId: "camp-logical-1", instantlyCampaignId: "inst-camp-1" },
    ]);
    mockWithdrawManualQualification.mockResolvedValue({
      withdrawn: true,
      qualification: {
        ...SAMPLE_BRONZE_ROW,
        replyKind: "lead_interested" as const,
        withdrawnAt: new Date("2026-08-28T10:00:00.000Z"),
        withdrawnBy: "user-1",
      },
    });

    const app = await createApp();
    const res = await request(app)
      .post("/orgs/manual-qualifications/withdrawals")
      .set("x-org-id", "org-1")
      .set("x-user-id", "user-1")
      .send({ ...body, notes: "picked the wrong kind" });

    expect(res.status).toBe(200);
    // What was stated is still readable — this is a correction, not an erasure.
    expect(res.body.qualification.status).toBe("lead_interested");
    expect(res.body.qualification.withdrawnAt).toBe("2026-08-28T10:00:00.000Z");
    expect(res.body.qualification.withdrawnBy).toBe("user-1");
    expect(mockWithdrawManualQualification).toHaveBeenCalledWith(
      expect.objectContaining({
        orgId: "org-1",
        instantlyCampaignId: "inst-camp-1",
        leadEmail: "lead@test.com",
        withdrawnBy: "user-1",
        notes: "picked the wrong kind",
      }),
    );
  });

  it("a listed withdrawn statement carries withdrawnAt so a consumer can tell it from a standing one", async () => {
    mockListManualQualifications.mockResolvedValue([
      {
        ...SAMPLE_BRONZE_ROW,
        withdrawnAt: new Date("2026-08-28T10:00:00.000Z"),
        withdrawnBy: "user-1",
      },
      { ...SAMPLE_BRONZE_ROW, id: "bronze-row-2", withdrawnAt: null, withdrawnBy: null },
    ]);

    const app = await createApp();
    const res = await request(app)
      .get("/orgs/manual-qualifications")
      .set("x-org-id", "org-1");

    expect(res.status).toBe(200);
    expect(res.body.qualifications[0].withdrawnAt).toBe("2026-08-28T10:00:00.000Z");
    expect(res.body.qualifications[1].withdrawnAt).toBeNull();
  });
});
