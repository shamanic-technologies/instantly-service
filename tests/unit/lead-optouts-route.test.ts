import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockRecord = vi.fn();
const mockWithdraw = vi.fn();
const mockList = vi.fn();

vi.mock("../../src/lib/lead-optouts", async () => {
  const actual = await vi.importActual<typeof import("../../src/lib/lead-optouts")>(
    "../../src/lib/lead-optouts",
  );
  return {
    ...actual,
    recordLeadOptOut: (...a: unknown[]) => mockRecord(...a),
    withdrawLeadOptOut: (...a: unknown[]) => mockWithdraw(...a),
    listLeadOptOuts: (...a: unknown[]) => mockList(...a),
  };
});

vi.mock("../../src/db", () => ({ db: {} }));
vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {},
  instantlyEvents: {},
  instantlyLeadOptoutsRaw: {},
  instantlyLeadOptoutWithdrawals: {},
}));

const STATED_AT = new Date("2026-09-01T09:00:00.000Z");
const ROW = {
  id: "optout-1",
  orgId: "org-1",
  email: "alice@media.com",
  channel: "sms" as const,
  statedBy: "user-1",
  notes: "texted me",
  statedAt: STATED_AT,
  withdrawnAt: null,
  withdrawnBy: null,
};

async function createApp(userId: string | null = "user-1") {
  const router = (await import("../../src/routes/lead-optouts")).default;
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => {
    res.locals.orgId = "org-1";
    if (userId) res.locals.userId = userId;
    next();
  });
  app.use(router);
  return app;
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("POST /orgs/opt-outs", () => {
  it("records the statement and reports what it stopped", async () => {
    mockRecord.mockResolvedValue({
      recorded: true,
      optOut: ROW,
      campaignsAffected: 2,
      campaignsStopped: 2,
    });

    const app = await createApp();
    const res = await request(app)
      .post("/")
      .send({ email: "alice@media.com", channel: "sms", notes: "texted me" });

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      idempotent: false,
      campaignsAffected: 2,
      campaignsStopped: 2,
      optOut: {
        email: "alice@media.com",
        channel: "sms",
        statedBy: "user-1",
        statedAt: STATED_AT.toISOString(),
        withdrawnAt: null,
      },
    });
    expect(mockRecord).toHaveBeenCalledWith(
      expect.objectContaining({ orgId: "org-1", leadEmail: "alice@media.com", channel: "sms", statedBy: "user-1" }),
    );
  });

  it("refuses a channel outside the vocabulary — an unauditable opt-out is not a consent record", async () => {
    const app = await createApp();
    const res = await request(app)
      .post("/")
      .send({ email: "alice@media.com", channel: "carrier_pigeon" });

    expect(res.status).toBe(400);
    expect(mockRecord).not.toHaveBeenCalled();
  });

  it("refuses when the channel is missing entirely", async () => {
    const app = await createApp();
    const res = await request(app).post("/").send({ email: "alice@media.com" });
    expect(res.status).toBe(400);
    expect(mockRecord).not.toHaveBeenCalled();
  });

  it("refuses without an author — a consent record with no author is not one", async () => {
    const app = await createApp(null);
    const res = await request(app)
      .post("/")
      .send({ email: "alice@media.com", channel: "sms" });

    expect(res.status).toBe(400);
    expect(mockRecord).not.toHaveBeenCalled();
  });

  it("reports an idempotent re-record without claiming it acted again", async () => {
    mockRecord.mockResolvedValue({
      recorded: false,
      optOut: ROW,
      campaignsAffected: 0,
      campaignsStopped: 0,
    });

    const app = await createApp();
    const res = await request(app)
      .post("/")
      .send({ email: "alice@media.com", channel: "sms" });

    expect(res.status).toBe(200);
    expect(res.body.idempotent).toBe(true);
  });
});

describe("POST /orgs/opt-outs/withdrawals", () => {
  it("returns the withdrawn record carrying who took it back and when", async () => {
    const withdrawnAt = new Date("2026-09-02T09:00:00.000Z");
    mockWithdraw.mockResolvedValue({
      withdrawn: true,
      optOut: { ...ROW, withdrawnAt, withdrawnBy: "user-2" },
      campaignsAffected: 1,
    });

    const app = await createApp("user-2");
    const res = await request(app).post("/withdrawals").send({ email: "alice@media.com" });

    expect(res.status).toBe(200);
    expect(res.body.optOut).toMatchObject({
      withdrawnAt: withdrawnAt.toISOString(),
      withdrawnBy: "user-2",
    });
  });

  it("refuses distinguishably when nothing stands", async () => {
    mockWithdraw.mockResolvedValue({ withdrawn: false, reason: "no_standing_optout" });

    const app = await createApp();
    const res = await request(app).post("/withdrawals").send({ email: "alice@media.com" });

    expect(res.status).toBe(404);
    expect(res.body.code).toBe("no_standing_optout");
  });
});

describe("GET /orgs/opt-outs", () => {
  it("returns the log, withdrawn records included and marked", async () => {
    const withdrawnAt = new Date("2026-09-02T09:00:00.000Z");
    mockList.mockResolvedValue([{ ...ROW, withdrawnAt, withdrawnBy: "user-2" }, ROW]);

    const app = await createApp();
    const res = await request(app).get("/");

    expect(res.status).toBe(200);
    expect(res.body.optOuts).toHaveLength(2);
    expect(res.body.optOuts[0].withdrawnAt).toBe(withdrawnAt.toISOString());
    expect(res.body.optOuts[1].withdrawnAt).toBeNull();
  });

  it("passes standing_only through", async () => {
    mockList.mockResolvedValue([]);
    const app = await createApp();
    await request(app).get("/?standing_only=true");
    expect(mockList).toHaveBeenCalledWith(expect.objectContaining({ standingOnly: true }));
  });
});
