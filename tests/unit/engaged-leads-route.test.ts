import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockFetch = vi.fn();

vi.mock("../../src/lib/engaged-leads", async () => {
  const actual = await vi.importActual<
    typeof import("../../src/lib/engaged-leads")
  >("../../src/lib/engaged-leads");
  return { ...actual, fetchEngagedLeads: (...a: unknown[]) => mockFetch(...a) };
});

vi.mock("../../src/db", () => ({ db: {} }));

const LEAD = {
  campaignId: "camp-1",
  instantlyCampaignId: "ic-1",
  leadEmail: "alice@media.com",
  brandIds: ["b-1"],
  engagedAt: "2026-09-01T10:00:00.000Z",
  replied: true,
  clicked: false,
  firstRepliedAt: "2026-09-01T10:00:00.000Z",
  firstClickedAt: null,
  replyClassification: "positive",
  replyKind: "lead_interested",
  disqualified: false,
};

async function createApp() {
  const router = (await import("../../src/routes/engaged-leads")).default;
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => {
    res.locals.orgId = "org-1";
    next();
  });
  app.use(router);
  return app;
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("GET /orgs/engaged-leads", () => {
  it("returns the engaged leads with a count", async () => {
    mockFetch.mockResolvedValue([LEAD]);
    const app = await createApp();

    const res = await request(app).get("/");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ success: true, count: 1, leads: [LEAD] });
  });

  it("returns 200 with an empty list when nobody has engaged", async () => {
    // An org whose leads have all stayed silent is a real, correct answer —
    // not a failure, and not a 404.
    mockFetch.mockResolvedValue([]);
    const app = await createApp();

    const res = await request(app).get("/");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ success: true, count: 0, leads: [] });
  });

  it("scopes to the caller's org, never to a caller-supplied one", async () => {
    mockFetch.mockResolvedValue([]);
    const app = await createApp();

    await request(app).get("/").query({ org_id: "someone-else" });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.objectContaining({ orgId: "org-1" }),
    );
  });

  it("threads the filters through", async () => {
    mockFetch.mockResolvedValue([]);
    const app = await createApp();

    await request(app).get("/").query({
      brand_id: "3f1a5b2c-1111-4aaa-8bbb-222222222222",
      campaign_id: "camp-1",
      since: "2026-08-01T00:00:00.000Z",
      limit: "25",
    });

    expect(mockFetch).toHaveBeenCalledWith({
      orgId: "org-1",
      brandId: "3f1a5b2c-1111-4aaa-8bbb-222222222222",
      campaignId: "camp-1",
      since: "2026-08-01T00:00:00.000Z",
      limit: 25,
    });
  });

  it("400s an invalid filter rather than silently ignoring it", async () => {
    mockFetch.mockResolvedValue([]);
    const app = await createApp();

    const res = await request(app).get("/").query({ brand_id: "not-a-uuid" });

    expect(res.status).toBe(400);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("500s on a read failure instead of reporting an empty list", async () => {
    // The one wrong answer that looks exactly like a correct one.
    mockFetch.mockRejectedValue(new Error("connection terminated"));
    const app = await createApp();

    const res = await request(app).get("/");

    expect(res.status).toBe(500);
    expect(res.body.error).toContain("connection terminated");
  });
});
