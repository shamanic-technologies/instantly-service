import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockExecute(...a) },
}));

import { buildCampaignSendQueue } from "../../src/lib/campaign-send-queue";
import { DEFAULT_LEAD_TIMEZONE, SEND_WINDOW_END_HOUR, SEND_WINDOW_START_HOUR } from "../../src/lib/sending-window";

function extractSqlText(obj: unknown): string {
  if (typeof obj === "string") return obj;
  if (obj == null) return "";
  if (Array.isArray(obj)) return obj.map(extractSqlText).join("");
  if (typeof obj === "object") {
    const o = obj as Record<string, unknown>;
    if (Array.isArray(o.value)) return o.value.join("");
    if (Array.isArray(o.queryChunks)) return extractSqlText(o.queryChunks);
    return Object.values(o).map(extractSqlText).join("");
  }
  return "";
}

async function createApp() {
  const router = (await import("../../src/routes/campaign-send-queue")).default;
  const app = express();
  app.use((_req, res, next) => {
    res.locals.orgId = "org-1";
    next();
  });
  app.use(router);
  return app;
}

beforeEach(() => {
  vi.clearAllMocks();
});

// Sunday 2026-10-18 20:00 UTC: closed everywhere in the Americas/Europe (Sunday is not a sending day).
const SUNDAY_EVENING = new Date("2026-10-18T20:00:00.000Z");

describe("buildCampaignSendQueue", () => {
  it("splits leads awaiting their first email from queued follow-ups and dates the next window per zone", () => {
    const q = buildCampaignSendQueue(
      ["c1"],
      [
        { campaignId: "c1", timezone: "America/New_York", deliveryStatus: "contacted", queuedSteps: 3 },
        { campaignId: "c1", timezone: "America/New_York", deliveryStatus: "contacted", queuedSteps: 3 },
        { campaignId: "c1", timezone: "Europe/Paris", deliveryStatus: "contacted", queuedSteps: 1 },
        { campaignId: "c1", timezone: "America/New_York", deliveryStatus: "sent", queuedSteps: 2 },
      ],
      SUNDAY_EVENING,
    );
    expect(q.leadsAwaitingFirstEmail).toBe(3);
    expect(q.leadsWithFollowUpQueued).toBe(1);
    expect(q.queuedEmails).toBe(9);
    // Paris opens Monday 07:00 local (05:00 UTC) before New York (11:00 UTC).
    expect(q.timezones).toEqual([
      { timezone: "Europe/Paris", timezoneIsDefault: false, leadsAwaitingFirstEmail: 1, windowOpensAt: "2026-10-19T05:00:00.000Z" },
      { timezone: "America/New_York", timezoneIsDefault: false, leadsAwaitingFirstEmail: 2, windowOpensAt: "2026-10-19T11:00:00.000Z" },
    ]);
    expect(q.firstEmailsFrom).toBe("2026-10-19T05:00:00.000Z");
    expect(q.sendWindow).toEqual({
      weekdays: ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday"],
      startHour: SEND_WINDOW_START_HOUR,
      endHour: SEND_WINDOW_END_HOUR,
      timezoneRule: "lead_local",
    });
  });

  it("a lead with no usable zone waits for the default zone, flagged", () => {
    const q = buildCampaignSendQueue(["c1"], [{ campaignId: "c1", timezone: null, deliveryStatus: "contacted", queuedSteps: 1 }], SUNDAY_EVENING);
    expect(q.timezones).toHaveLength(1);
    expect(q.timezones[0].timezone).toBe(DEFAULT_LEAD_TIMEZONE);
    expect(q.timezones[0].timezoneIsDefault).toBe(true);
  });

  it("nothing awaiting = no first-email date, never a guessed one", () => {
    const q = buildCampaignSendQueue(["c1"], [{ campaignId: "c1", timezone: "Europe/Paris", deliveryStatus: "sent", queuedSteps: 1 }], SUNDAY_EVENING);
    expect(q.leadsAwaitingFirstEmail).toBe(0);
    expect(q.firstEmailsFrom).toBeNull();
    expect(q.timezones).toEqual([]);
  });

  it("inside an open window the next send is now", () => {
    const mondayNoonParis = new Date("2026-10-19T10:00:00.000Z");
    const q = buildCampaignSendQueue(["c1"], [{ campaignId: "c1", timezone: "Europe/Paris", deliveryStatus: "contacted", queuedSteps: 1 }], mondayNoonParis);
    expect(q.firstEmailsFrom).toBe(mondayNoonParis.toISOString());
  });
});

describe("GET /orgs/send-queue", () => {
  it("reads the org's live queued sequences for the campaigns", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [{ campaignId: "c1", timezone: "Europe/Paris", deliveryStatus: "contacted", queuedSteps: 2 }] });
    const app = await createApp();
    const res = await request(app).get("/").query({ campaignIds: "c1,c2,c1" });
    expect(res.status).toBe(200);
    expect(res.body.campaignIds).toEqual(["c1", "c2"]);
    expect(res.body.leadsAwaitingFirstEmail).toBe(1);
    expect(res.body.queuedEmails).toBe(2);
    const text = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(text).toContain("c.org_id =");
    expect(text).toContain("sc.status = 'provisioned'");
    expect(text).toContain("c.status = 'active'");
  });

  it("400 without campaignIds", async () => {
    const app = await createApp();
    const res = await request(app).get("/");
    expect(res.status).toBe(400);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("500 on a read failure, never zeros", async () => {
    mockExecute.mockRejectedValueOnce(new Error("db down"));
    const app = await createApp();
    const res = await request(app).get("/").query({ campaignIds: "c1" });
    expect(res.status).toBe(500);
    expect(res.body.leadsAwaitingFirstEmail).toBeUndefined();
  });
});
