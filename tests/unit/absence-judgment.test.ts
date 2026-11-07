import { beforeEach, describe, expect, it, vi } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockDbExecute(...a) } }));
const mockPlatformComplete = vi.fn();
vi.mock("../../src/lib/chat-client", () => ({
  platformComplete: (...a: unknown[]) => mockPlatformComplete(...a),
}));
const mockRunFor = vi.fn();
vi.mock("../../src/lib/reply-judging-run", () => ({
  replyJudgingRunForSequence: (...a: unknown[]) => mockRunFor(...a),
}));
const mockFindLead = vi.fn();
const mockFindBrandRow = vi.fn();
const mockReadDate = vi.fn();
const mockStateDate = vi.fn();
vi.mock("../../src/lib/lead-client", () => ({
  findLeadOnCampaignByEmail: (...a: unknown[]) => mockFindLead(...a),
  findLeadRowOfBrandByEmail: (...a: unknown[]) => mockFindBrandRow(...a),
  readLeadFollowupDate: (...a: unknown[]) => mockReadDate(...a),
  stateLeadFollowupDate: (...a: unknown[]) => mockStateDate(...a),
}));

import {
  absenceJudgmentRequest,
  judgeAbsence,
  stateHoldAsFollowupDate,
} from "../../src/lib/self-send/absence-judgment";

/** Every bound value of the drizzle `sql` the code ran, flattened. */
function boundValues(call: unknown[]): unknown[] {
  const q = call[0] as { queryChunks?: unknown[] };
  const out: unknown[] = [];
  const walk = (chunks: unknown[] | undefined) => {
    for (const c of chunks ?? []) {
      if (c && typeof c === "object" && "queryChunks" in (c as object)) walk((c as { queryChunks: unknown[] }).queryChunks);
      else if (c && typeof c === "object" && "value" in (c as object) && Array.isArray((c as { value: unknown }).value)) continue;
      else out.push(c);
    }
  };
  walk(q.queryChunks);
  return out;
}

const RAJESH = {
  imapMessageId: "imap-1",
  instantlyCampaignId: "self:69535282-400f-456a-8475-fe4a46f298c6",
  orgId: "org-1",
  leadEmail: "prospect@example.com",
  receivedAt: new Date("2026-10-21T12:15:52Z"),
  subject: "Automatic reply: [EXTERNAL] Proving AI value at McKesson?",
  text: "Hi All,\nI am on PTO from 10/16 to 10/26. During my absence, I kindly request you to reach out to respective POC's as mentioned below.",
};

beforeEach(() => {
  vi.clearAllMocks();
  mockDbExecute.mockResolvedValue({ rows: [] });
  mockRunFor.mockResolvedValue("run-judging-1");
});

describe("judgeAbsence: read once through chat-service, stored whatever the outcome", () => {
  it("asks chat-service on the org's reply-judging run (PLATFORM-CALLER-RUN-1024) and stores the hold", async () => {
    mockPlatformComplete.mockResolvedValue({
      content: "",
      json: { date: { year: null, month: 10, day: 26 }, meaning: "away_through" },
      tokensInput: 1,
      tokensOutput: 1,
      model: "deepseek-flash",
    });
    const { holdUntil } = await judgeAbsence(RAJESH);
    expect(holdUntil?.toISOString()).toBe("2026-10-28T00:00:00.000Z");
    expect(mockRunFor).toHaveBeenCalledWith(RAJESH.instantlyCampaignId, { fallbackOrgId: "org-1" });
    const [request, caller] = mockPlatformComplete.mock.calls[0]!;
    expect(caller).toEqual({ runId: "run-judging-1" });
    expect(request).toMatchObject({ provider: "deepseek", model: "deepseek-flash", responseFormat: "json", temperature: 0 });
    const values = boundValues(mockDbExecute.mock.calls[0]!);
    expect(values).toContain("return_date");
    expect(values).toContain("2026-10-27");
    expect(values).toContain("2026-10-28T00:00:00.000Z");
    expect(values).toContain("imap-1");
  });

  it("no date: stored as judged (no_date), no hold", async () => {
    mockPlatformComplete.mockResolvedValue({ content: '{"date":null,"meaning":null}', tokensInput: 1, tokensOutput: 1, model: "m" });
    const { holdUntil } = await judgeAbsence({ ...RAJESH, text: "I am out of the office at present." });
    expect(holdUntil).toBeNull();
    expect(boundValues(mockDbExecute.mock.calls[0]!)).toContain("no_date");
  });

  it("a failed call stores nothing (asked again next pass)", async () => {
    mockPlatformComplete.mockRejectedValue(new Error("chat-service 502"));
    await expect(judgeAbsence(RAJESH)).rejects.toThrow("502");
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("the request strips quoted history: a date in OUR quoted email is never read", () => {
    const req = absenceJudgmentRequest({
      ...RAJESH,
      text: "I am out of the office at present.\n\nOn Tue, Oct 21, 2026 Kevin wrote:\n> Shall we talk before 11/30?",
    });
    expect(req.message).toContain("I am out of the office at present.");
    expect(req.message).not.toContain("11/30");
  });
});

describe("stateHoldAsFollowupDate (AC4: the next follow-up date is never before the return)", () => {
  const hold = {
    id: "abs-1",
    orgId: "org-1",
    campaignId: "camp-1",
    brandIds: ["brand-1"],
    leadEmail: "prospect@example.com",
    firstDayBack: "2026-10-27",
    holdUntil: new Date("2026-10-28T00:00:00Z"),
  };

  beforeEach(() => {
    mockFindLead.mockResolvedValue({ id: "row-1", email: "prospect@example.com" });
  });

  it("an earlier date is moved to the hold", async () => {
    mockReadDate.mockResolvedValue({ nextFollowupAt: "2026-10-24T12:30:00Z", basis: "send" });
    mockStateDate.mockResolvedValue({ nextFollowupAt: "2026-10-28T00:00:00Z", basis: "stated" });
    await expect(stateHoldAsFollowupDate(hold)).resolves.toBe("brand-1:stated");
    expect(mockStateDate).toHaveBeenCalledWith({
      orgId: "org-1",
      leadRowId: "row-1",
      brandId: "brand-1",
      nextFollowupAt: "2026-10-28T00:00:00.000Z",
      reason: "Out of office, back on 2026-10-27",
    });
  });

  it("a later date is kept (never moved earlier)", async () => {
    mockReadDate.mockResolvedValue({ nextFollowupAt: "2026-11-15T00:00:00Z", basis: "stated" });
    await expect(stateHoldAsFollowupDate(hold)).resolves.toBe("brand-1:already_later");
    expect(mockStateDate).not.toHaveBeenCalled();
  });

  it("a stop is kept: an out-of-office never restarts nor closes follow-ups", async () => {
    mockReadDate.mockResolvedValue({ nextFollowupAt: null, basis: "stopped" });
    await expect(stateHoldAsFollowupDate(hold)).resolves.toBe("brand-1:kept_stopped");
    expect(mockStateDate).not.toHaveBeenCalled();
  });

  it("a platform send (no caller campaign) moves the date through ANY row of the person at the brand", async () => {
    mockFindBrandRow.mockResolvedValue({ id: "row-b" });
    mockReadDate.mockResolvedValue({ nextFollowupAt: "2026-10-24T12:30:00Z", basis: "send" });
    mockStateDate.mockResolvedValue({ nextFollowupAt: "2026-10-28T00:00:00Z", basis: "stated" });
    await expect(stateHoldAsFollowupDate({ ...hold, campaignId: null })).resolves.toBe("brand-1:stated");
    expect(mockFindLead).not.toHaveBeenCalled();
    expect(mockFindBrandRow).toHaveBeenCalledWith({ orgId: "org-1", brandId: "brand-1", email: "prospect@example.com" });
    expect(mockStateDate.mock.calls[0][0].leadRowId).toBe("row-b");
  });

  it("mike@rxwellness.com 2026-11-07: the caller campaign holds no row: the date still moves through the brand's row, never left due", async () => {
    mockFindLead.mockResolvedValue(null);
    mockFindBrandRow.mockResolvedValue({ id: "row-other-campaign" });
    mockReadDate.mockResolvedValue({ nextFollowupAt: "2026-10-24T12:30:00Z", basis: "send" });
    mockStateDate.mockResolvedValue({ nextFollowupAt: "2026-10-28T00:00:00Z", basis: "stated" });
    await expect(stateHoldAsFollowupDate(hold)).resolves.toBe("brand-1:stated");
    expect(mockStateDate.mock.calls[0][0].leadRowId).toBe("row-other-campaign");
  });

  it("no row of the person anywhere at the brand: said per brand", async () => {
    mockFindLead.mockResolvedValue(null);
    mockFindBrandRow.mockResolvedValue(null);
    await expect(stateHoldAsFollowupDate(hold)).resolves.toBe("brand-1:no_lead_row");
    expect(mockStateDate).not.toHaveBeenCalled();
  });

  it("lead-service failing throws (retried next pass)", async () => {
    mockReadDate.mockRejectedValue(new Error("lead-service GET 502"));
    await expect(stateHoldAsFollowupDate(hold)).rejects.toThrow("502");
  });
});
