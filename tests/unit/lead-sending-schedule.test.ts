import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockExecute(...a) },
}));

import { buildLeadSendingSchedule } from "../../src/lib/lead-sending-schedule";
import {
  DEFAULT_LEAD_TIMEZONE,
  SEND_WINDOW_END_HOUR,
  SEND_WINDOW_START_HOUR,
} from "../../src/lib/sending-window";

/** node-postgres shape: a QueryResult, never an array. */
function pgResult(rows: Record<string, unknown>[]) {
  return { rows, rowCount: rows.length };
}

async function createApp() {
  const router = (await import("../../src/routes/lead-sending-schedule")).default;
  const app = express();
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

describe("buildLeadSendingSchedule", () => {
  it("serves the send path's own window constants", () => {
    const s = buildLeadSendingSchedule("Europe/Paris", true);
    expect(s).toEqual({
      weekdays: ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday"],
      startHour: SEND_WINDOW_START_HOUR,
      endHour: SEND_WINDOW_END_HOUR,
      timezone: "Europe/Paris",
      timezoneIsDefault: false,
      hasSequence: true,
    });
  });

  it("canonicalizes a legacy spelling to the primary zone, still the lead's own", () => {
    const s = buildLeadSendingSchedule("Asia/Calcutta", true);
    expect(s.timezone).toBe("Asia/Kolkata");
    expect(s.timezoneIsDefault).toBe(false);
  });

  it("falls back to the default zone, flagged, when none is stored", () => {
    for (const raw of [null, "", "   "]) {
      const s = buildLeadSendingSchedule(raw, true);
      expect(s.timezone).toBe(DEFAULT_LEAD_TIMEZONE);
      expect(s.timezoneIsDefault).toBe(true);
    }
  });

  it("flags an unusable stored zone as default rather than serving garbage", () => {
    const s = buildLeadSendingSchedule("Not/AZone", true);
    expect(s.timezone).toBe(DEFAULT_LEAD_TIMEZONE);
    expect(s.timezoneIsDefault).toBe(true);
  });
});

describe("GET /orgs/sending-schedule", () => {
  it("returns the lead's own zone, not default", async () => {
    mockExecute.mockResolvedValue(pgResult([{ timezone: "America/New_York" }]));
    const app = await createApp();

    const res = await request(app).get("/").query({ email: "Alice@Media.com" });

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.schedule).toMatchObject({
      timezone: "America/New_York",
      timezoneIsDefault: false,
      hasSequence: true,
      startHour: 7,
      endHour: 19,
    });
  });

  it("returns the default schedule for a lead we hold nothing for (never 404)", async () => {
    mockExecute.mockResolvedValue(pgResult([]));
    const app = await createApp();

    const res = await request(app).get("/").query({ email: "nobody@x.com" });

    expect(res.status).toBe(200);
    expect(res.body.schedule).toMatchObject({
      timezone: DEFAULT_LEAD_TIMEZONE,
      timezoneIsDefault: true,
      hasSequence: false,
    });
  });

  it("returns the default zone, flagged, for a sequence with no stored zone", async () => {
    mockExecute.mockResolvedValue(pgResult([{ timezone: null }]));
    const app = await createApp();

    const res = await request(app).get("/").query({ email: "a@b.com" });

    expect(res.body.schedule).toMatchObject({
      timezoneIsDefault: true,
      hasSequence: true,
    });
  });

  it("400s without an email and on a non-uuid brand_id", async () => {
    const app = await createApp();
    expect((await request(app).get("/")).status).toBe(400);
    expect(
      (await request(app).get("/").query({ email: "a@b.com", brand_id: "x" })).status,
    ).toBe(400);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("fails loud on a DB error instead of serving the default", async () => {
    mockExecute.mockRejectedValue(new Error("boom"));
    const app = await createApp();

    const res = await request(app).get("/").query({ email: "a@b.com" });

    expect(res.status).toBe(500);
    expect(res.body.error).toBe("boom");
  });
});
