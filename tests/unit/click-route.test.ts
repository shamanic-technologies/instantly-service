import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockInsertValues = vi.fn();
const mockPromoteEvent = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({
      values: (row: unknown) => Promise.resolve(mockInsertValues(row)),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({ trackingHitsRaw: { id: "id" } }));

vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...args: unknown[]) => mockPromoteEvent(...args),
}));

const SECRET = "click-route-test-secret";
process.env.SELF_SEND_UNSUBSCRIBE_SECRET = SECRET;
process.env.SELF_SEND_PUBLIC_URL = "https://links.test";

const { default: clickRoutes } = await import("../../src/routes/click");
const { buildClickPath } = await import("../../src/lib/self-send/click-tracking");

const TARGET = {
  instantlyCampaignId: "self:abc",
  leadEmail: "prospect@example.com",
  step: 1,
  url: "https://brand.example/landing",
};
const VALID_PATH = buildClickPath(TARGET, SECRET);

const CHROME =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";

function app() {
  const a = express();
  a.set("trust proxy", 1);
  a.use("/c", clickRoutes);
  return a;
}

beforeEach(() => {
  vi.resetAllMocks();
  mockInsertValues.mockReturnValue([{ id: "hit-1" }]);
});

describe("GET /c/:payload/:signature", () => {
  it("NEVER promotes a silver event — the verdict is not available at request time", async () => {
    const res = await request(app()).get(VALID_PATH).set("user-agent", CHROME);

    expect(res.status).toBe(302);
    expect(res.headers.location).toBe(TARGET.url);
    expect(mockPromoteEvent).not.toHaveBeenCalled();
  });

  it("records a plausible browser hit UNDECIDED, so the sweep can settle it", async () => {
    await request(app()).get(VALID_PATH).set("user-agent", CHROME);

    const row = mockInsertValues.mock.calls[0][0];
    expect(row.kind).toBe("click");
    expect(row.classification).toBeNull();
    expect(row.classificationReason).toBeNull();
  });

  it("records a scanner user-agent with its reason, and still redirects", async () => {
    const res = await request(app())
      .get(VALID_PATH)
      .set("user-agent", "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)");

    expect(res.status).toBe(302);
    const row = mockInsertValues.mock.calls[0][0];
    expect(row.classification).toBe("scanner");
    expect(row.classificationReason).toBe("scanner_user_agent");
    expect(mockPromoteEvent).not.toHaveBeenCalled();
  });

  it("stores the REAL client address from X-Forwarded-For, not the proxy's", async () => {
    await request(app())
      .get(VALID_PATH)
      .set("user-agent", CHROME)
      .set("x-forwarded-for", "203.0.113.9");

    const row = mockInsertValues.mock.calls[0][0];
    expect(row.clientIp).toBe("203.0.113.9");
    expect(row.payload.ip).toBe("203.0.113.9");
    expect(row.payload.forwardedFor).toBe("203.0.113.9");
  });

  it("404s a bad signature without recording anything", async () => {
    const res = await request(app()).get("/notapayload/notasignature");
    expect(res.status).toBe(404);
    expect(mockInsertValues).not.toHaveBeenCalled();
  });
});
