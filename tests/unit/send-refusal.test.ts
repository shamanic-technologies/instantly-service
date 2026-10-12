import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockExecute(...args) },
}));

const mockOptOuts = vi.fn();
const mockNotAProspects = vi.fn();
vi.mock("../../src/lib/lead-optouts", () => ({
  OPT_OUT_REFUSAL_CODE: "lead_opted_out",
  findStandingOptOuts: (...a: unknown[]) => mockOptOuts(...a),
}));
vi.mock("../../src/lib/not-a-prospect", () => ({
  NOT_A_PROSPECT_REFUSAL_CODE: "lead_not_a_prospect",
  findNotAProspects: (...a: unknown[]) => mockNotAProspects(...a),
}));

import { findSendRefusals } from "../../src/lib/send-refusal";
import { findRecentBrandContact, findRecentBrandContacts } from "../../src/lib/recontact-window";

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

const LAST = "2026-07-13T13:03:53.417Z";
const LAPSE = "2026-10-13T13:03:53.417Z";

describe("findSendRefusals — what POST /orgs/send would refuse", () => {
  beforeEach(() => {
    mockExecute.mockReset();
    mockOptOuts.mockReset().mockResolvedValue(new Map());
    mockNotAProspects.mockReset().mockResolvedValue(new Map());
  });

  it("a person emailed for the brand inside the window: recent_brand_contact until the lapse Postgres computed", async () => {
    mockExecute.mockResolvedValue({
      rows: [{ lead_email: "jjohnson@purewellchiro.com", brand_id: "b1", last_emailed_at: LAST, lapses_at: LAPSE }],
    });
    const out = await findSendRefusals("org-1", ["b1"], ["JJohnson@PureWellChiro.com", "clean@x.com"]);
    // keyed by the address AS ASKED, matched case-insensitively like the gate
    expect(out.get("JJohnson@PureWellChiro.com")).toEqual({ code: "recent_brand_contact", until: LAPSE });
    expect(out.has("clean@x.com")).toBe(false);
  });

  it("an opt-out never lapses and outlasts a window", async () => {
    mockOptOuts.mockResolvedValue(new Map([["a@b.com", {}]]));
    mockExecute.mockResolvedValue({
      rows: [{ lead_email: "a@b.com", brand_id: "b1", last_emailed_at: LAST, lapses_at: LAPSE }],
    });
    const out = await findSendRefusals("org-1", ["b1"], ["a@b.com"]);
    expect(out.get("a@b.com")).toEqual({ code: "lead_opted_out", until: null });
  });

  it("not a prospect never lapses and outlasts a window", async () => {
    mockNotAProspects.mockResolvedValue(new Map([["a@b.com", { brandId: "b1", replyKind: "lead_is_client" }]]));
    mockExecute.mockResolvedValue({
      rows: [{ lead_email: "a@b.com", brand_id: "b1", last_emailed_at: LAST, lapses_at: LAPSE }],
    });
    const out = await findSendRefusals("org-1", ["b1"], ["a@b.com"]);
    expect(out.get("a@b.com")).toEqual({ code: "lead_not_a_prospect", until: null });
  });

  it("no brand: only the org-wide opt-out can stand (the window is per brand)", async () => {
    mockOptOuts.mockResolvedValue(new Map([["a@b.com", {}]]));
    const out = await findSendRefusals("org-1", [], ["a@b.com", "c@d.com"]);
    expect(mockExecute).not.toHaveBeenCalled();
    expect(out.get("a@b.com")).toEqual({ code: "lead_opted_out", until: null });
    expect(out.has("c@d.com")).toBe(false);
  });

  it("fails loud on a DB error", async () => {
    mockExecute.mockRejectedValue(new Error("db down"));
    await expect(findSendRefusals("org-1", ["b1"], ["a@b.com"])).rejects.toThrow("db down");
  });
});

describe("re-contact window — the send gate and the status read run ONE query", () => {
  beforeEach(() => mockExecute.mockReset().mockResolvedValue({ rows: [] }));

  it("same SQL for one address and for many", async () => {
    await findRecentBrandContact("a@b.com", ["b1"]);
    await findRecentBrandContacts(["a@b.com", "c@d.com"], ["b1"]);
    const { PgDialect } = await import("drizzle-orm/pg-core");
    const dialect = new PgDialect();
    const [single, batch] = mockExecute.mock.calls.map((c) => dialect.sqlToQuery(c[0] as never).sql);
    expect(single).toBe(batch);
    expect(single).toContain("e.inferred = false");
    expect(single).toContain("e.timestamp > now() - interval '3 months'");
    // the lapse handles a month without that day (Aug 31 + 3 months)
    expect(single).toContain("date_trunc('month'");
  });
});
