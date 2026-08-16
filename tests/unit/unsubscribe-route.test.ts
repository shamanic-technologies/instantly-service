import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockInsertValues = vi.fn();
const mockPromoteEvent = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({
      values: (row: unknown) => ({
        returning: () => Promise.resolve(mockInsertValues(row)),
      }),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  trackingHitsRaw: { id: "id" },
}));

vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...args: unknown[]) => mockPromoteEvent(...args),
}));

const SECRET = "route-test-secret";
process.env.SELF_SEND_UNSUBSCRIBE_SECRET = SECRET;
process.env.SELF_SEND_PUBLIC_URL = "https://opt.test";

const { default: unsubscribeRoutes } = await import("../../src/routes/unsubscribe");
const { buildUnsubscribePath } = await import("../../src/lib/self-send/unsubscribe");

const IDENTITY = { instantlyCampaignId: "camp-1", leadEmail: "prospect@example.com" };
const VALID_PATH = buildUnsubscribePath(IDENTITY, SECRET);

function app() {
  const a = express();
  a.use("/u", unsubscribeRoutes);
  return a;
}

beforeEach(() => {
  vi.resetAllMocks();
  mockInsertValues.mockReturnValue([{ id: "hit-1" }]);
  mockPromoteEvent.mockResolvedValue({ promoted: true, silverEventId: "ev-1" });
});

describe("GET /u/:payload/:signature", () => {
  // The load-bearing one. Corporate link scanners fetch every URL in an inbound
  // email before the human sees it, so acting on GET would opt out prospects who
  // never clicked and silently kill their sequence.
  it("does NOT unsubscribe — it only offers a POST form", async () => {
    const res = await request(app()).get(`/u${VALID_PATH.slice(2)}`);

    expect(res.status).toBe(200);
    expect(res.text).toContain("<form method=\"post\">");
    expect(mockPromoteEvent).not.toHaveBeenCalled();
  });

  it("still records the hit — a scanner arriving first is itself evidence", async () => {
    await request(app()).get(`/u${VALID_PATH.slice(2)}`).set("user-agent", "ScannerBot/1.0");

    expect(mockInsertValues).toHaveBeenCalledTimes(1);
    const row = mockInsertValues.mock.calls[0]![0] as Record<string, unknown>;
    expect(row.kind).toBe("unsubscribe");
    expect(row.method).toBe("GET");
    expect(row.userAgent).toBe("ScannerBot/1.0");
  });

  it("404s an invalid signature without recording or promoting anything", async () => {
    const [, , payload] = VALID_PATH.split("/");
    const res = await request(app()).get(`/u/${payload}/forged`);

    expect(res.status).toBe(404);
    expect(mockInsertValues).not.toHaveBeenCalled();
    expect(mockPromoteEvent).not.toHaveBeenCalled();
  });

  // The route must not become an oracle for which campaigns exist.
  it("gives an unknown campaign the same 404 as a bad signature", async () => {
    const other = buildUnsubscribePath(
      { instantlyCampaignId: "does-not-exist", leadEmail: "x@y.com" },
      "a-different-secret",
    );
    const res = await request(app()).get(`/u${other.slice(2)}`);

    expect(res.status).toBe(404);
  });
});

describe("POST /u/:payload/:signature", () => {
  it("promotes a real lead_unsubscribed event through the shared path", async () => {
    const res = await request(app()).post(`/u${VALID_PATH.slice(2)}`);

    expect(res.status).toBe(200);
    expect(mockPromoteEvent).toHaveBeenCalledTimes(1);

    const input = mockPromoteEvent.mock.calls[0]![0] as Record<string, unknown>;
    expect(input.eventType).toBe("lead_unsubscribed");
    expect(input.instantlyCampaignId).toBe("camp-1");
    expect(input.leadEmail).toBe("prospect@example.com");
    expect(input.source).toBe("self_send");
    // Real, not inferred — the sequence stop and the hold cancels depend on it.
    expect(input.inferred).toBeUndefined();
    // An opt-out is about the whole sequence; claiming a step would assert we
    // know which email they were reading, and we do not.
    expect(input.step).toBeNull();
  });

  it("gives the silver event honest bronze provenance", async () => {
    await request(app()).post(`/u${VALID_PATH.slice(2)}`);

    const input = mockPromoteEvent.mock.calls[0]![0] as Record<string, unknown>;
    expect(input.sourceRowId).toBe("hit-1");
  });

  it("404s a forged signature without promoting", async () => {
    const [, , payload] = VALID_PATH.split("/");
    const res = await request(app()).post(`/u/${payload}/forged`);

    expect(res.status).toBe(404);
    expect(mockPromoteEvent).not.toHaveBeenCalled();
  });

  it("escapes the recipient address in the rendered page", async () => {
    const xss = buildUnsubscribePath(
      { instantlyCampaignId: "camp-1", leadEmail: '<script>alert(1)</script>@x.com' },
      SECRET,
    );
    const res = await request(app()).post(`/u${xss.slice(2)}`);

    expect(res.text).not.toContain("<script>");
    expect(res.text).toContain("&lt;script&gt;");
  });
});
