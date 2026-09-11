import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
const mockRefreshGold = vi.fn();

vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockDbExecute(...args) },
}));

vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...args: unknown[]) => mockRefreshGold(...args),
}));

const { backfillScannerClicks } = await import(
  "../../src/lib/self-send/click-scanner-backfill"
);

const CHROME =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";

function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

function sqlText(node: unknown): string {
  if (typeof node === "string") return node;
  if (Array.isArray(node)) return node.map(sqlText).join("");
  if (node && typeof node === "object") {
    const obj = node as Record<string, unknown>;
    if (Array.isArray(obj.queryChunks)) return sqlText(obj.queryChunks);
    if (typeof obj.value === "string") return obj.value;
    if (Array.isArray(obj.value)) return sqlText(obj.value);
  }
  return "";
}

function hitRow(over: Record<string, unknown> = {}) {
  return {
    id: "hit-1",
    instantly_campaign_id: "self:abc",
    lead_email: "prospect@example.com",
    method: "GET",
    user_agent: CHROME,
    brand_id: "brand-opsfolio",
    has_paired_unsubscribe: true,
    ...over,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  mockRefreshGold.mockResolvedValue(undefined);
});

describe("backfillScannerClicks — dry run", () => {
  it("reports per-brand counts and writes NOTHING", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        hitRow(),
        hitRow({ id: "hit-2", lead_email: "other@example.com" }),
        hitRow({
          id: "hit-3",
          lead_email: "human@example.com",
          has_paired_unsubscribe: false,
          brand_id: "brand-kevin",
        }),
      ]),
    );

    const summary = await backfillScannerClicks();

    expect(summary).toMatchObject({
      dryRun: true,
      hitsExamined: 3,
      scannerHits: 2,
      humanHits: 1,
      leadsDemoted: 2,
      silverEventsRemoved: 0,
    });
    expect(summary.byBrand).toEqual([
      { brandId: "brand-opsfolio", scannerHits: 2, scannerLeads: 2, humanHits: 0, humanLeads: 0 },
      { brandId: "brand-kevin", scannerHits: 0, scannerLeads: 0, humanHits: 1, humanLeads: 1 },
    ]);
    // Exactly one query: the candidate SELECT. No delete, no update, no gold.
    expect(mockDbExecute).toHaveBeenCalledTimes(1);
    expect(mockRefreshGold).not.toHaveBeenCalled();
  });

  it("only considers self-send LEGACY click hits", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));
    await backfillScannerClicks();

    const text = sqlText(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("h.kind = 'click'");
    expect(text).toContain("h.classification = 'legacy'");
  });
});

describe("backfillScannerClicks — commit", () => {
  it("removes the scanner's silver click and its inferred children, then rebuilds gold", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([hitRow()]));
    // delete click → delete inferred → mark hit
    mockDbExecute.mockResolvedValueOnce(pgResult([{ id: "ev-click" }]));
    mockDbExecute.mockResolvedValueOnce(pgResult([{ id: "ev-open" }]));
    mockDbExecute.mockResolvedValue(pgResult([]));

    const summary = await backfillScannerClicks({ dryRun: false });

    expect(summary).toMatchObject({
      silverEventsRemoved: 1,
      inferredEventsRemoved: 1,
      goldRowsRefreshed: 1,
    });
    expect(mockRefreshGold).toHaveBeenCalledWith("self:abc", "prospect@example.com");

    const deleteText = sqlText(mockDbExecute.mock.calls[1][0]);
    // Instantly-sourced clicks are a different, clean era — never touched.
    expect(deleteText).toContain("source = 'self_send'");
    expect(deleteText).toContain("event_type = 'email_link_clicked'");
    expect(deleteText).toContain("source_row_id");

    const inferredText = sqlText(mockDbExecute.mock.calls[2][0]);
    // A real send that merely got UPGRADED in place must survive.
    expect(inferredText).toContain("inferred = TRUE");
  });

  it("leaves a human hit's silver event alone", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([hitRow({ has_paired_unsubscribe: false })]),
    );
    mockDbExecute.mockResolvedValue(pgResult([]));

    const summary = await backfillScannerClicks({ dryRun: false });

    expect(summary).toMatchObject({ humanHits: 1, silverEventsRemoved: 0, leadsDemoted: 0 });
    expect(mockRefreshGold).not.toHaveBeenCalled();
    const markText = sqlText(mockDbExecute.mock.calls[1][0]);
    expect(markText).toContain("UPDATE tracking_hits_raw");
  });

  it("keeps a lead that also has a real click out of the demoted count", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        hitRow({ id: "scan" }),
        hitRow({ id: "real", has_paired_unsubscribe: false }),
      ]),
    );
    mockDbExecute.mockResolvedValue(pgResult([]));

    const summary = await backfillScannerClicks({ dryRun: true });
    expect(summary).toMatchObject({ scannerHits: 1, humanHits: 1, leadsDemoted: 0 });
  });
});
