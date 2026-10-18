import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockDbExecute(...args) },
}));
vi.mock("../../src/lib/runs-client", () => ({ incurredCosts: vi.fn() }));

import {
  incurredItemsForSend,
  incurredSendCostKey,
  runIncurredSendCostsBackfill,
  toUnits,
  unitsToUsd,
  SENDS_PER_CALL,
} from "../../src/lib/incurred-send-costs-backfill";
import type { IncurredCostItemRequest, IncurredCostsResult } from "../../src/lib/runs-client";

/** node-postgres shape: `{ rows }`, never an array. */
const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length });

const sendRow = (i: number, orgId = "org-a") => ({
  holdId: `hold-${String(i).padStart(5, "0")}`,
  runId: `run-${i}`,
  orgId,
  userId: "user-1",
  sentAt: "2026-09-02T14:31:07.000Z",
});

/** A runs-service answer pricing every item at 2.8403 cents, in `status`. */
function answer(mode: "preview" | "apply", items: IncurredCostItemRequest[], status: string | null = "refunded"): IncurredCostsResult {
  return {
    itemCount: items.length,
    toRecordCount: items.length,
    alreadyRecordedCount: 0,
    invalidCount: 0,
    totalCostInUsdCents: (items.length * 2.8403).toFixed(10),
    netTotalCostInUsdCents: (items.length * 2.8403).toFixed(10),
    alreadyRecordedTotalCostInUsdCents: "0.0000000000",
    unitPrices: { "instantly-account-email-sent": "2.8403000000", "instantly-domain-email-sent": "2.8403000000" },
    ...(mode === "apply" ? { recordedCount: items.length } : {}),
    items: items.map((it) => ({
      runId: it.runId,
      idempotencyKey: it.idempotencyKey,
      costName: it.costName,
      outcome: mode === "apply" ? "recorded" : "to_record",
      costId: mode === "apply" ? `c-${it.idempotencyKey}` : null,
      status: mode === "apply" ? status : null,
      incurredAt: it.incurredAt,
      totalCostInUsdCents: "2.8403000000",
      netCostInUsdCents: "2.8403000000",
      error: null,
    })),
  };
}

/** db.execute: first call(s) = keyset pages, last = the not-declarable count. */
function mockPages(pages: unknown[][]) {
  mockDbExecute.mockReset();
  for (const page of pages) mockDbExecute.mockResolvedValueOnce(pgResult(page));
  mockDbExecute.mockResolvedValueOnce(pgResult([]));
  mockDbExecute.mockResolvedValue(pgResult([{ noSendEvidence: 0, noOrg: 0 }]));
}

describe("incurred send costs backfill (SCOUT-VISIT-SCREEN-1018)", () => {
  beforeEach(() => mockDbExecute.mockReset());

  it("declares the two email costs every send of the era carried, on the step's own run, at its real send instant", () => {
    const items = incurredItemsForSend({ holdId: "h1", runId: "r1", orgId: "o", userId: "u", sentAt: new Date("2026-09-02T14:31:07Z") });
    expect(items).toEqual([
      { runId: "r1", costName: "instantly-account-email-sent", quantity: 1, idempotencyKey: "instantly-incurred-send-cost:h1:account", incurredAt: "2026-09-02T14:31:07.000Z" },
      { runId: "r1", costName: "instantly-domain-email-sent", quantity: 1, idempotencyKey: "instantly-incurred-send-cost:h1:domain", incurredAt: "2026-09-02T14:31:07.000Z" },
    ]);
  });

  it("keys are stable per (hold, side), so a re-run records nothing twice", () => {
    expect(incurredSendCostKey("h1", "account")).toBe(incurredSendCostKey("h1", "account"));
    expect(incurredSendCostKey("h1", "account")).not.toBe(incurredSendCostKey("h1", "domain"));
  });

  it("decimal sums are exact and round to the cent", () => {
    expect(toUnits("2.8403000000") * 3n).toBe(toUnits("8.5209"));
    expect(unitsToUsd(toUnits("290700.4999999999"))).toBe("2907.00");
    expect(unitsToUsd(toUnits("290700.5"))).toBe("2907.01");
    expect(unitsToUsd(0n)).toBe("0.00");
  });

  it("dry run previews every send in batches of 500 items, writes nothing, sums per org", async () => {
    const page1 = Array.from({ length: SENDS_PER_CALL }, (_, i) => sendRow(i, i % 2 ? "org-b" : "org-a"));
    const page2 = [sendRow(SENDS_PER_CALL, "org-a")];
    mockPages([page1, page2]);
    const call = vi.fn(async (mode: "preview" | "apply", items: IncurredCostItemRequest[]) => answer(mode, items));

    const s = await runIncurredSendCostsBackfill({ dryRun: true }, call);

    expect(call).toHaveBeenCalledTimes(2);
    expect(call.mock.calls.every((c) => c[0] === "preview")).toBe(true);
    expect(call.mock.calls[0][1]).toHaveLength(500);
    expect(s.sendsSeen).toBe(251);
    expect(s.itemsToRecord).toBe(502);
    expect(s.itemsRecorded).toBe(0);
    // 502 x 2.8403 cents = $14.258306 -> $14.26
    expect(s.totalIncurredUsd).toBe("14.26");
    expect(s.chargedByThisRunUsd).toBe("0.00");
    expect(s.byOrg.map((o) => [o.orgId, o.sends])).toEqual([["org-a", 126], ["org-b", 125]]);
    expect(s.byOrg.every((o) => o.chargedByThisRunUsd === "0.00")).toBe(true);
  });

  it("the keyset walk resumes after the last hold of each page", async () => {
    mockPages([[sendRow(1), sendRow(2)]]);
    const call = vi.fn(async (mode: "preview" | "apply", items: IncurredCostItemRequest[]) => answer(mode, items));
    await runIncurredSendCostsBackfill({ dryRun: true }, call);
    const secondQuery = JSON.stringify(mockDbExecute.mock.calls[1][0]);
    expect(secondQuery).toContain("hold-00002");
  });

  it("apply passes the audit motive and counts recorded rows", async () => {
    mockPages([[sendRow(1), sendRow(2)]]);
    const call = vi.fn(async (mode: "preview" | "apply", items: IncurredCostItemRequest[]) => answer(mode, items));
    const s = await runIncurredSendCostsBackfill({ dryRun: false }, call);
    expect(call.mock.calls[0][0]).toBe("apply");
    expect(call.mock.calls[0][2].reason).toMatch(/never charged/);
    expect(s.itemsRecorded).toBe(4);
    expect(s.chargedByThisRunUsd).toBe("0.00");
  });

  it("FAILS LOUD when a written row sits in a charged state (control: the refunded row passes)", async () => {
    mockPages([[sendRow(1)]]);
    const charged = vi.fn(async (mode: "preview" | "apply", items: IncurredCostItemRequest[]) => answer(mode, items, "actual"));
    await expect(runIncurredSendCostsBackfill({ dryRun: false }, charged)).rejects.toThrow(/CHARGED state/);
  });

  it("respects the limit", async () => {
    mockPages([[sendRow(1), sendRow(2), sendRow(3)]]);
    const call = vi.fn(async (mode: "preview" | "apply", items: IncurredCostItemRequest[]) => answer(mode, items));
    await runIncurredSendCostsBackfill({ dryRun: true, limit: 3 }, call);
    expect(call).toHaveBeenCalledTimes(1);
    // limit reached: no further page read before the count
    expect(mockDbExecute).toHaveBeenCalledTimes(2);
  });
});
