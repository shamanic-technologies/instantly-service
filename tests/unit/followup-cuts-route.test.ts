import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockCut = vi.fn();
const mockList = vi.fn();

vi.mock("../../src/lib/followup-cuts", async () => {
  const actual = await vi.importActual<typeof import("../../src/lib/followup-cuts")>("../../src/lib/followup-cuts");
  return {
    ...actual,
    cutFollowups: (...a: unknown[]) => mockCut(...a),
    listFollowupCuts: (...a: unknown[]) => mockList(...a),
  };
});
vi.mock("../../src/db", () => ({ db: {} }));

const { LeadsNotInCampaignError } = await import("../../src/lib/followup-cuts");

async function createApp(userId: string | null = "user-1") {
  const router = (await import("../../src/routes/followup-cuts")).default;
  const app = express();
  app.use(express.json());
  app.use((_req, res, next) => {
    res.locals.orgId = "org-1";
    if (userId) res.locals.userId = userId;
    next();
  });
  app.use("/orgs/followup-cuts", router);
  return app;
}

const BODY = { campaignId: "camp-1", leadEmails: ["a@x.com"], reason: "sourcing bug" };

beforeEach(() => vi.resetAllMocks());

describe("POST /orgs/followup-cuts", () => {
  it("400 without x-user-id: the audit must name who cut", async () => {
    const res = await request(await createApp(null)).post("/orgs/followup-cuts").send(BODY);
    expect(res.status).toBe(400);
    expect(mockCut).not.toHaveBeenCalled();
  });

  it("400 without a reason", async () => {
    const res = await request(await createApp()).post("/orgs/followup-cuts").send({ ...BODY, reason: undefined });
    expect(res.status).toBe(400);
  });

  it("200 with per-lead outcomes", async () => {
    mockCut.mockResolvedValue([
      { email: "a@x.com", outcome: "cut", holdsCancelled: 2, cutAt: "t", cutBy: "user-1" },
    ]);
    const res = await request(await createApp()).post("/orgs/followup-cuts").send(BODY);
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ campaignId: "camp-1", cut: 1, alreadyCut: 0, failed: 0 });
    expect(mockCut.mock.calls[0][0]).toMatchObject({ orgId: "org-1", userId: "user-1", campaignId: "camp-1" });
  });

  it("502 when any lead failed", async () => {
    mockCut.mockResolvedValue([{ email: "a@x.com", outcome: "failed", holdsCancelled: 0, cutAt: null, cutBy: null, error: "x" }]);
    const res = await request(await createApp()).post("/orgs/followup-cuts").send(BODY);
    expect(res.status).toBe(502);
    expect(res.body.failed).toBe(1);
  });

  it("404 leads_not_in_campaign names the strangers", async () => {
    mockCut.mockRejectedValue(new LeadsNotInCampaignError(["b@y.com"]));
    const res = await request(await createApp()).post("/orgs/followup-cuts").send(BODY);
    expect(res.status).toBe(404);
    expect(res.body).toMatchObject({ code: "leads_not_in_campaign", emails: ["b@y.com"] });
  });

  it("500 on an unexpected throw (never a hung socket)", async () => {
    mockCut.mockRejectedValue(new Error("db down"));
    const res = await request(await createApp()).post("/orgs/followup-cuts").send(BODY);
    expect(res.status).toBe(500);
  });
});

describe("GET /orgs/followup-cuts", () => {
  it("lists the campaign's cuts", async () => {
    mockList.mockResolvedValue([
      { campaignId: "camp-1", leadEmail: "a@x.com", reason: "r", cutBy: "u", holdsCancelled: 2, cutAt: new Date("2026-10-13T00:00:00.000Z") },
    ]);
    const res = await request(await createApp()).get("/orgs/followup-cuts?campaignId=camp-1");
    expect(res.status).toBe(200);
    expect(res.body.cuts[0]).toEqual({ campaignId: "camp-1", email: "a@x.com", reason: "r", cutBy: "u", holdsCancelled: 2, cutAt: "2026-10-13T00:00:00.000Z" });
  });
});
