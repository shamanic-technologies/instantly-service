import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: { execute: (q: unknown) => mockExecute(q) },
}));

import { refreshLeadStatusCurrent } from "../../src/lib/status-gold";

/** Recursively concatenate every string fragment in a drizzle SQL query. */
function chunkText(query: unknown): string {
  if (query == null) return "";
  if (typeof query === "string") return query;
  if (typeof query !== "object") return String(query);
  const chunks = (query as { queryChunks?: unknown[] }).queryChunks;
  if (Array.isArray(chunks)) return chunks.map(chunkText).join("");
  const v = (query as { value?: unknown }).value;
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return v.map(chunkText).join("");
  return "";
}

beforeEach(() => {
  vi.clearAllMocks();
  mockExecute.mockResolvedValue({ rows: [] });
});

describe("the gold status row carries the reply KIND, not only its coarse class", () => {
  it("derives reply_kind from the reply-kind events of this lead", async () => {
    await refreshLeadStatusCurrent("inst-1", "lead@test.com");
    const sql = chunkText(mockExecute.mock.calls[0][0]);
    expect(sql).toContain("reply_kind");
    expect(sql).toContain("rk.event_type AS reply_kind");
    expect(sql).toContain("reply_kind = EXCLUDED.reply_kind");
  });

  it("lets a HUMAN statement win outright, mirroring the reply_classification manual pin", async () => {
    await refreshLeadStatusCurrent("inst-1", "lead@test.com");
    const sql = chunkText(mockExecute.mock.calls[0][0]);
    // manual first, THEN recency — not the other way round, or a webhook
    // arriving after a human statement would outrank the human while the coarse
    // column beside it stayed pinned to the human's value.
    const order = sql.slice(sql.indexOf("ORDER BY (k.source = 'manual') DESC"));
    expect(order.startsWith("ORDER BY (k.source = 'manual') DESC, k.timestamp DESC")).toBe(true);
  });

  it("skips a WITHDRAWN statement — nobody stands behind it", async () => {
    await refreshLeadStatusCurrent("inst-1", "lead@test.com");
    const sql = chunkText(mockExecute.mock.calls[0][0]);
    expect(sql).toContain("k.withdrawn_at IS NULL");
  });

  it("stops counting a WITHDRAWN opt-out as an unsubscribe", async () => {
    await refreshLeadStatusCurrent("inst-1", "lead@test.com");
    const sql = chunkText(mockExecute.mock.calls[0][0]);
    expect(sql).toContain("e.event_type <> 'lead_unsubscribed' OR e.withdrawn_at IS NULL");
  });

  it("does NOT apply that filter to every event type — a withdrawn qualification never retracted the reply", async () => {
    await refreshLeadStatusCurrent("inst-1", "lead@test.com");
    const sql = chunkText(mockExecute.mock.calls[0][0]);
    // The join must not carry a blanket `AND e.withdrawn_at IS NULL`, which
    // would silently flip `replied` back to false for a lead whose reply KIND
    // was withdrawn — something no withdrawal ever claimed.
    expect(sql).not.toMatch(/AND\s+e\.withdrawn_at IS NULL/);
  });
});
