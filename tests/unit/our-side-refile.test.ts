import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockDbExecute(...a) } }));
const mockRetract = vi.fn();
vi.mock("../../src/lib/self-send/forwarded-replies", () => ({
  retractRowEvents: (...a: unknown[]) => mockRetract(...a),
}));
const mockRefresh = vi.fn();
vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...a: unknown[]) => mockRefresh(...a),
}));
const mockLoadOurSide = vi.fn();
vi.mock("../../src/lib/self-send/our-side-sender", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  loadOurSide: (...a: unknown[]) => mockLoadOurSide(...a),
}));
vi.mock("../../src/lib/forward-positive-reply", () => ({
  POSITIVE_QUALIFICATION_EVENT_TYPES: new Set(["lead_interested", "lead_info_requested", "lead_meeting_requested"]),
}));

import { refileOurSideReplies } from "../../src/lib/self-send/our-side-refile";

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

const DEV_ROW = {
  id: "464c89f2",
  instantly_campaign_id: "self:d1306a8c",
  lead_email: "jerryclark@alpinespinalrehab.com",
  from_address: '"Dev-Raj Mandroop" <dev@docdinners.com>',
  at: new Date("2026-10-14T19:47:00Z"),
  events: ["lead_meeting_requested"],
};
const JERRY_ROW = {
  id: "c3cfa1f2",
  instantly_campaign_id: "self:d1306a8c",
  lead_email: "jerryclark@alpinespinalrehab.com",
  from_address: '"Alpine Spinal Rehab" <jerryclark@alpinespinalrehab.com>',
  at: new Date("2026-10-12T23:13:23Z"),
  events: ["lead_interested", "reply_received"],
};
const JTS_ROW = {
  id: "jts1",
  instantly_campaign_id: "self:jts",
  lead_email: "jts@etn.travel",
  from_address: "jts@travelnewsgroup.com",
  at: new Date("2026-10-10T00:00:00Z"),
  events: ["reply_received"],
};

beforeEach(() => {
  vi.resetAllMocks();
  mockLoadOurSide.mockResolvedValue({
    addresses: new Set(["dev@docdinners.com"]),
    domains: new Set(["docdinners.com", "gemkernelio.com"]),
  });
  mockDbExecute.mockResolvedValue(pgResult([]));
});

describe("refileOurSideReplies — taking back replies our side wrote", () => {
  it("dry run names Dev's message only: Jerry's own reply and a prospect's second address stay", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([JERRY_ROW, DEV_ROW, JTS_ROW]));

    const summary = await refileOurSideReplies();

    expect(summary.dryRun).toBe(true);
    expect(summary.rowsRead).toBe(3);
    expect(summary.refiled.map((r) => r.rowId)).toEqual(["464c89f2"]);
    expect(summary.refiled[0]).toMatchObject({ events: ["lead_meeting_requested"] });
    expect(mockRetract).not.toHaveBeenCalled();
    expect(mockDbExecute).toHaveBeenCalledTimes(1);
  });

  it("commit retracts what it promoted, refiles it ours (kept in history), rebuilds gold, frees the claim", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([DEV_ROW]));

    const summary = await refileOurSideReplies({ dryRun: false });

    expect(summary.refiled).toHaveLength(1);
    expect(mockRetract).toHaveBeenCalledWith("464c89f2", "our-side-refiled:reply->staff_reply");
    const sqlText = mockDbExecute.mock.calls.map((c) => JSON.stringify(c[0])).join("\n");
    expect(sqlText).toContain("UPDATE imap_messages_raw");
    expect(sqlText).toContain("UPDATE messages SET kind");
    expect(sqlText).toContain("meeting_request_celebrated_at = NULL");
    // Never deleted: the message stays in the conversation.
    expect(sqlText).not.toMatch(/DELETE FROM imap_messages_raw|DELETE FROM messages/);
    expect(mockRefresh).toHaveBeenCalledWith("self:d1306a8c", "jerryclark@alpinespinalrehab.com");
  });
});
