import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
const mockPromoteEvent = vi.fn();

vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockDbExecute(...args) },
}));

vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...args: unknown[]) => mockPromoteEvent(...args),
}));

const { promotePendingClicks } = await import("../../src/lib/self-send/click-promotion");

const CHROME =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";

/** Flatten a drizzle SQL object (including nested `sql.raw` chunks) to its text. */
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

/** node-postgres hands back a QueryResult OBJECT, never a bare array. */
function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

function hitRow(over: Record<string, unknown> = {}) {
  return {
    id: "hit-1",
    instantly_campaign_id: "self:abc",
    lead_email: "prospect@example.com",
    step: 1,
    method: "GET",
    user_agent: CHROME,
    received_at: "2026-09-11T10:00:00.000Z",
    has_paired_unsubscribe: false,
    ...over,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  mockPromoteEvent.mockResolvedValue({ promoted: true, silverEventId: "ev-1" });
});

describe("promotePendingClicks", () => {
  it("promotes a human click with the HIT's own timestamp, then marks the bronze row", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([hitRow()]));
    mockDbExecute.mockResolvedValue(pgResult([]));

    const summary = await promotePendingClicks({ asOf: new Date("2026-09-11T10:05:00Z") });

    expect(summary).toMatchObject({ decided: 1, promoted: 1, scanner: 0, failed: 0 });
    expect(mockPromoteEvent).toHaveBeenCalledTimes(1);
    const input = mockPromoteEvent.mock.calls[0][0];
    expect(input.eventType).toBe("email_link_clicked");
    expect(input.source).toBe("self_send");
    expect(input.sourceRowId).toBe("hit-1");
    // Idempotence rests on this: the dedupe index keys on the timestamp, so a
    // re-promotion of the same hit must present the same instant, not `now()`.
    expect(input.timestamp.toISOString()).toBe("2026-09-11T10:00:00.000Z");
  });

  it("does NOT promote a hit whose lead also fetched the opt-out link in the window", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([hitRow({ has_paired_unsubscribe: true })]));
    mockDbExecute.mockResolvedValue(pgResult([]));

    const summary = await promotePendingClicks();

    expect(mockPromoteEvent).not.toHaveBeenCalled();
    expect(summary).toMatchObject({ scanner: 1, promoted: 0 });
    expect(summary.reasons.paired_unsubscribe_fetch).toBe(1);
  });

  it("does NOT promote a HEAD request or a scanner user-agent", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        hitRow({ id: "hit-head", method: "HEAD" }),
        hitRow({
          id: "hit-ua",
          user_agent: "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0)",
        }),
      ]),
    );
    mockDbExecute.mockResolvedValue(pgResult([]));

    const summary = await promotePendingClicks();

    expect(mockPromoteEvent).not.toHaveBeenCalled();
    expect(summary).toMatchObject({ decided: 2, scanner: 2, promoted: 0 });
  });

  it("leaves a failed hit UNDECIDED so the next tick retries it, and counts the failure", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([hitRow()]));
    mockDbExecute.mockResolvedValue(pgResult([]));
    mockPromoteEvent.mockRejectedValueOnce(new Error("runs-service down"));

    const summary = await promotePendingClicks();

    expect(summary).toMatchObject({ failed: 1, promoted: 0, decided: 0 });
    // SELECTs only — no UPDATE marked the hit, so it stays a candidate.
    expect(mockDbExecute.mock.calls.map((c) => sqlText(c[0])).join("\n")).not.toContain("UPDATE");
  });

  it("asks the network question AFTER this tick's own scanner verdicts are written (Defender, Olive 2026-10-02)", async () => {
    // Same Azure IP, 12 s apart, both pending in the same tick: the unreduced UA
    // is a scanner on its face; the Mac one only through its network.
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        hitRow({
          id: "hit-aadit",
          user_agent:
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.7444.163 Safari/537.36",
        }),
        hitRow({
          id: "hit-bhemelaar",
          user_agent:
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        }),
      ]),
    );
    mockDbExecute.mockImplementation(async (query: unknown) => {
      const text = sqlText(query);
      if (text.includes("s.classification = 'scanner'")) {
        return pgResult([{ id: "hit-bhemelaar" }]);
      }
      return pgResult([]);
    });

    const summary = await promotePendingClicks();

    expect(mockPromoteEvent).not.toHaveBeenCalled();
    expect(summary).toMatchObject({ decided: 2, scanner: 2, promoted: 0 });
    expect(summary.reasons).toMatchObject({ scanner_user_agent: 1, scanner_network: 1 });

    const texts = mockDbExecute.mock.calls.map((c) => sqlText(c[0]));
    const firstMark = texts.findIndex((t) => t.includes("UPDATE tracking_hits_raw"));
    const networkQuery = texts.findIndex((t) => t.includes("s.classification = 'scanner'"));
    expect(firstMark).toBeGreaterThan(0);
    expect(networkQuery).toBeGreaterThan(firstMark);
  });

  it("negative control: a human click on a network with no scanner verdict is still promoted", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([hitRow()]));
    mockDbExecute.mockResolvedValue(pgResult([]));

    const summary = await promotePendingClicks();

    expect(summary).toMatchObject({ promoted: 1, scanner: 0 });
  });

  it("selects only undecided CLICK hits past the hold, matching the lead case-folded", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));
    await promotePendingClicks({ asOf: new Date("2026-09-11T10:05:00Z") });

    const query = mockDbExecute.mock.calls[0][0];
    const text = sqlText(query);
    expect(text).toContain("h.kind = 'click'");
    expect(text).toContain("h.classification IS NULL");
    expect(text).toContain("u.kind = 'unsubscribe'");
    expect(text).toContain("lower(u.lead_email) = lower(h.lead_email)");
    expect(text).toContain("interval '120 seconds'");
    expect(text).toContain("interval '60 seconds'");
  });
});
