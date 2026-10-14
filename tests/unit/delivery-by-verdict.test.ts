import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockExecute(...args) },
}));

const mockCaughtUpAt = vi.fn();
const mockSync = vi.fn();
vi.mock("../../src/lib/verification-history-mirror", () => ({
  readMirrorCaughtUpAt: () => mockCaughtUpAt(),
  syncVerificationHistory: () => mockSync(),
}));

import { assembleDeliveryByVerdict, VERDICT_BUCKETS } from "../../src/lib/delivery-by-verdict";
import { clearStatsCache } from "../../src/lib/stats-cache";

const THROUGH = new Date("2026-10-14T10:00:00Z");

function pgResult<T>(rows: T[]) {
  return { rows, rowCount: rows.length, command: "SELECT", fields: [] };
}

describe("assembleDeliveryByVerdict", () => {
  it("serves every bucket, zero when empty, in the fixed order", () => {
    const out = assembleDeliveryByVerdict(
      [
        { bucket: "valid", sent: 100, bounced: 1 },
        { bucket: "never_verified", sent: 50, bounced: 5 },
      ],
      { sent: 150, bounced: 6, since: "2026-02-19T01:15:19Z", sent_all: 160 },
      THROUGH,
      "2026-09-25T09:56:21Z",
    );
    expect(out.buckets.map((b) => b.verdict)).toEqual([...VERDICT_BUCKETS]);
    expect(out.buckets[0]).toEqual({ verdict: "valid", sent: 100, delivered: 99, bounced: 1, deliveryRate: 0.99, bounceRate: 0.01 });
    const risky = out.buckets.find((b) => b.verdict === "risky")!;
    expect(risky).toEqual({ verdict: "risky", sent: 0, delivered: 0, bounced: 0, deliveryRate: null, bounceRate: null });
    expect(out.total).toEqual({ sent: 150, delivered: 144, bounced: 6, deliveryRate: 0.96, bounceRate: 0.04 });
    expect(out.basis).toBe("sequences");
    expect(out.sentThrough).toBe(THROUGH.toISOString());
    expect(out.sentAfterThrough).toBe(10);
    expect(out.since).toBe("2026-02-19T01:15:19.000Z");
  });

  it("fails loud when the buckets do not add up to the independent total", () => {
    expect(() =>
      assembleDeliveryByVerdict([{ bucket: "valid", sent: 10, bounced: 0 }], { sent: 11, bounced: 0, since: null, sent_all: 11 }, THROUGH, null),
    ).toThrow(/do not reconcile/);
  });

  it("fails loud on a verdict outside the closed set, never folds it", () => {
    expect(() =>
      assembleDeliveryByVerdict([{ bucket: "deliverable", sent: 3, bounced: 0 }], { sent: 3, bounced: 0, since: null, sent_all: 3 }, THROUGH, null),
    ).toThrow(/not a known bucket/);
  });

  it("reads pg's string counts as numbers", () => {
    const out = assembleDeliveryByVerdict(
      [{ bucket: "catch_all", sent: "4" as unknown as number, bounced: "1" as unknown as number }],
      { sent: "4" as unknown as number, bounced: "1" as unknown as number, since: null, sent_all: "4" as unknown as number },
      THROUGH,
      null,
    );
    expect(out.total.sent).toBe(4);
    expect(out.buckets[1].deliveryRate).toBe(0.75);
  });
});

async function makeApp() {
  const router = (await import("../../src/routes/delivery-by-verdict")).default;
  const app = express();
  app.use(express.json());
  app.use("/internal", router);
  return app;
}

describe("GET /internal/delivery-by-verdict", () => {
  beforeEach(() => {
    clearStatsCache();
    mockExecute.mockReset();
    mockCaughtUpAt.mockReset();
  });

  it("503 until the verification history has been mirrored once (never reads everyone as never verified)", async () => {
    mockCaughtUpAt.mockResolvedValue(null);
    const res = await request(await makeApp()).get("/internal/delivery-by-verdict");
    expect(res.status).toBe(503);
    expect(res.body.code).toBe("verification_history_not_synced");
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("serves the split cut at the mirror's caught-up instant", async () => {
    mockCaughtUpAt.mockResolvedValue(THROUGH);
    mockExecute
      .mockResolvedValueOnce(pgResult([{ bucket: "valid", sent: 9, bounced: 1 }, { bucket: "no_verdict", sent: 1, bounced: 1 }]))
      .mockResolvedValueOnce(pgResult([{ bucket: "valid", sent: 6, bounced: 0 }, { bucket: "invalid", sent: 4, bounced: 2 }]))
      .mockResolvedValueOnce(pgResult([{ sent: 10, bounced: 2, since: "2026-02-19T00:00:00Z", sent_all: 12 }]))
      .mockResolvedValueOnce(pgResult([{ since: "2026-09-25T00:00:00Z" }]));
    const res = await request(await makeApp()).get("/internal/delivery-by-verdict");
    expect(res.status).toBe(200);
    expect(res.body.total).toEqual({ sent: 10, delivered: 8, bounced: 2, deliveryRate: 0.8, bounceRate: 0.2 });
    expect(res.body.buckets).toHaveLength(7);
    expect(res.body.sentAfterThrough).toBe(2);
    expect(res.body.buckets.find((b: any) => b.verdict === "valid").sent).toBe(9);
    // Same sequences, bucketed by the verdict known today (incl. after the send).
    expect(res.body.bucketsByLatestVerdict).toHaveLength(7);
    expect(res.body.bucketsByLatestVerdict.find((b: any) => b.verdict === "invalid")).toMatchObject({ sent: 4, bounced: 2, deliveryRate: 0.5 });
    // The cutoff reaches both grouped reads and the total; only the first reads verdicts at send.
    const texts = mockExecute.mock.calls.slice(0, 3).map((c) => JSON.stringify(c[0]));
    for (const t of texts) expect(t).toContain(THROUGH.toISOString());
    expect(texts[0]).toContain("v.verified_at <= seq.first_sent_at");
    expect(texts[1]).not.toContain("v.verified_at <= seq.first_sent_at");
  });

  it("500 when the partition does not reconcile", async () => {
    mockCaughtUpAt.mockResolvedValue(THROUGH);
    mockExecute
      .mockResolvedValueOnce(pgResult([{ bucket: "valid", sent: 9, bounced: 1 }]))
      .mockResolvedValueOnce(pgResult([{ bucket: "valid", sent: 9, bounced: 1 }]))
      .mockResolvedValueOnce(pgResult([{ sent: 10, bounced: 1, since: null, sent_all: 10 }]))
      .mockResolvedValueOnce(pgResult([{ since: null }]));
    const res = await request(await makeApp()).get("/internal/delivery-by-verdict");
    expect(res.status).toBe(500);
    expect(res.body.error).toMatch(/do not reconcile/);
  });
});
