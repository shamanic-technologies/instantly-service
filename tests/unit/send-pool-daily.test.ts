import { describe, it, expect, vi, beforeEach } from "vitest";

// SEND-POOL-VENDOR-402-1030 — the send pool as a stored daily series: a day with
// a few hours at 0 must read 0, a day with no sample is null (never a fake 0).

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));
vi.mock("../../src/lib/send-lead", () => ({ fetchSendablePool: vi.fn() }));

import { buildSendPoolHistory, countByVendor, recordSendPools } from "../../src/lib/send-pool-daily";

beforeEach(() => execute.mockReset());

describe("send pool daily series", () => {
  it("emits every day of the window per pool, shared first; a missing day is null", () => {
    const history = buildSendPoolHistory(
      [
        { day: "2026-10-30", pool: "sales-crm-email-outreach", sendableMin: 3, sendableLast: 3, byVendor: { gandi: 3 } },
        { day: "2026-10-29", pool: "shared", sendableMin: 0, sendableLast: 0, byVendor: {} },
        { day: "2026-10-31", pool: "shared", sendableMin: 13, sendableLast: 13, byVendor: { primeforge: "13" } },
      ],
      "2026-10-29",
      "2026-10-31",
    );
    expect(history.map((h) => h.pool)).toEqual(["shared", "sales-crm-email-outreach"]);
    expect(history[0].days).toEqual([
      { day: "2026-10-29", sendableMin: 0, sendableLast: 0, byVendor: {} },
      { day: "2026-10-30", sendableMin: null, sendableLast: null, byVendor: null },
      { day: "2026-10-31", sendableMin: 13, sendableLast: 13, byVendor: { primeforge: 13 } },
    ]);
  });

  it("counts sendable mailboxes per vendor", () => {
    expect(countByVendor([{ infraProvider: "primeforge" }, { infraProvider: "primeforge" }, { infraProvider: null }])).toEqual({
      primeforge: 2,
      unclassified: 1,
    });
  });

  it("keeps the day's LOWEST sample and the latest per-vendor split", async () => {
    execute.mockResolvedValue({ rows: [] });
    await recordSendPools([{ pool: "shared", sendable: 0, byVendor: {} }], new Date("2026-10-30T16:00:00Z"));
    const query = JSON.stringify(execute.mock.calls[0][0]);
    expect(query).toContain("LEAST(send_pool_daily.sendable_min, EXCLUDED.sendable_min)");
    expect(query).toContain("ON CONFLICT (day, pool)");
  });
});
