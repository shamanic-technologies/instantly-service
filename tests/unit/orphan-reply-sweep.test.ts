import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockDbExecute(...a) } }));
const promoteEventMock = vi.fn(async () => ({ promoted: true, silverEventId: "e1" }));
vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...a: unknown[]) => promoteEventMock(...(a as [])),
}));
const judgmentMock = vi.fn();
vi.mock("../../src/lib/chat-client", () => ({
  platformJudgment: (...a: unknown[]) => judgmentMock(...(a as [])),
}));
vi.mock("../../src/lib/self-send/own-mail-domains", () => ({
  loadOwnMailDomains: async () => new Set(["saviolabsco.com"]),
}));
const actOnInboundReplyMock = vi.fn(async () => {});
vi.mock("../../src/lib/self-send/imap-poller", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  actOnInboundReply: (...a: unknown[]) => actOnInboundReplyMock(...(a as [])),
}));

import { runOrphanReplySweep } from "../../src/lib/self-send/orphan-reply-sweep";

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

const STACY_ROW = {
  id: "row-stacy",
  accountEmail: "michaela@saviolabsco.com",
  messageId: "<x@chsmetabolismdoc.com>",
  fromAddress: '"Stacy Blecher" <drblecher@chsmetabolismdoc.com>',
  subject: "Doc Dinners",
  headers: { from: "Stacy Blecher <drblecher@chsmetabolismdoc.com>" },
  text: "Hi Michaela, I would love to hear more about the dinners.",
  receivedAt: "2026-09-24T14:56:55Z",
  polledAt: "2026-09-24T15:05:47Z",
};

/** Route each query by what it reads, so the test does not pin call order. */
function database(rows: Array<Record<string, unknown>>) {
  mockDbExecute.mockImplementation(async (query: unknown) => {
    const text = JSON.stringify(query);
    if (text.includes("UPDATE imap_messages_raw")) return pgResult([]);
    if (text.includes("FROM imap_messages_raw")) return pgResult(rows);
    if (text.includes("FROM smtp_dispatch_raw")) {
      return pgResult([
        { instantlyCampaignId: "self:stacy", rawStep: "1", sentAt: "2026-09-24T12:54:44Z", source: "smtp" },
        // Sent AFTER her answer: the reply is filed on step 1, the step she had read.
        { instantlyCampaignId: "self:stacy", rawStep: "2", sentAt: "2026-09-28T12:17:40Z", source: "smtp" },
        { instantlyCampaignId: "self:john", rawStep: "1", sentAt: "2026-09-20T10:00:00Z", source: "smtp" },
      ]);
    }
    if (text.includes("instantly_leads")) {
      return pgResult([
        {
          instantlyCampaignId: "self:stacy",
          leadEmail: "stacy.blecher@twinhealth.com",
          orgId: "org-doc",
          brandIds: ["75d7e3e8-6926-4f85-a557-976895400666"],
          firstName: "Stacy",
          lastName: "Blecher",
          companyName: "Twin Health",
          subject: "Growing a holistic practice in Charleston",
          bodyHtml: "<p>Hi Stacy,</p><p>Doc Dinners hosts dinners.</p>",
        },
        {
          instantlyCampaignId: "self:john",
          leadEmail: "john@clinic.com",
          orgId: "org-doc",
          brandIds: [],
          firstName: "John",
          lastName: "Doe",
          companyName: "Clinic",
          subject: "Dinners in Austin",
          bodyHtml: "<p>Hi John</p>",
        },
      ]);
    }
    return pgResult([]);
  });
}

function updates(): string[] {
  return mockDbExecute.mock.calls
    .map((c) => JSON.stringify(c[0]))
    .filter((t) => t.includes("UPDATE imap_messages_raw"));
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("runOrphanReplySweep", () => {
  it("re-files a stranger's email the judgment ties to a lead as that lead's reply, and acts on it", async () => {
    database([STACY_ROW]);
    judgmentMock.mockResolvedValue({
      model: "m",
      answers: {
        orphan_reply: { type: "choice", choice: "lead_1", confidence: 0.9, probabilities: { lead_1: 0.94, none: 0.06 } },
      },
      usage: { inputTokens: 900, outputTokens: 0 },
    });

    const summary = await runOrphanReplySweep();

    expect(judgmentMock).toHaveBeenCalledTimes(1);
    const asked = JSON.stringify(judgmentMock.mock.calls[0]);
    expect(asked).toContain("stacy.blecher@twinhealth.com");
    // John shares nothing with the message: never offered, never paid for.
    expect(asked).not.toContain("john@clinic.com");

    expect(summary).toMatchObject({ judged: 1, matched: 1, judgmentInputTokens: 900 });
    expect(summary.recovered[0]).toMatchObject({
      leadEmail: "stacy.blecher@twinhealth.com",
      brandIds: ["75d7e3e8-6926-4f85-a557-976895400666"],
    });

    const [update] = updates();
    expect(update).toContain("kind = 'reply'");
    expect(update).toContain("orphanJudgment");

    // `reply_received` stops the sequence, on the step she had actually read.
    expect(promoteEventMock).toHaveBeenCalledWith(
      expect.objectContaining({
        eventType: "reply_received",
        instantlyCampaignId: "self:stacy",
        step: 1,
        timestamp: new Date("2026-09-24T14:56:55Z"),
        sourceRowId: "row-stacy",
      }),
    );
    expect(actOnInboundReplyMock).toHaveBeenCalledWith(
      expect.objectContaining({
        send: expect.objectContaining({ instantlyCampaignId: "self:stacy", orgId: "org-doc", step: 1 }),
        text: STACY_ROW.text,
      }),
    );
  });

  it("persists a `none` verdict so the message is never judged twice, and acts on nothing", async () => {
    database([STACY_ROW]);
    judgmentMock.mockResolvedValue({
      model: "m",
      answers: {
        orphan_reply: { type: "choice", choice: "none", confidence: 0.9, probabilities: { lead_1: 0.04, none: 0.96 } },
      },
      usage: { inputTokens: 900, outputTokens: 0 },
    });

    const summary = await runOrphanReplySweep();

    expect(summary).toMatchObject({ judged: 1, none: 1, matched: 0 });
    const [update] = updates();
    expect(update).toContain("orphanJudgment");
    expect(update).not.toContain("kind = 'reply'");
    expect(promoteEventMock).not.toHaveBeenCalled();
    expect(actOnInboundReplyMock).not.toHaveBeenCalled();
  });

  it("does not act on a shaky pick", async () => {
    database([STACY_ROW]);
    judgmentMock.mockResolvedValue({
      model: "m",
      answers: {
        orphan_reply: { type: "choice", choice: "lead_1", confidence: 0.1, probabilities: { lead_1: 0.6, none: 0.4 } },
      },
      usage: { inputTokens: 900, outputTokens: 0 },
    });

    const summary = await runOrphanReplySweep();

    expect(summary).toMatchObject({ lowConfidence: 1, matched: 0 });
    expect(promoteEventMock).not.toHaveBeenCalled();
  });

  it("never pays a judgment for warmup traffic or a newsletter", async () => {
    database([
      { ...STACY_ROW, id: "w", subject: "Stacy - coffee? | RXYQDJD WNT6JJB" },
      { ...STACY_ROW, id: "n", headers: { "list-unsubscribe": "<https://x>" } },
    ]);

    const summary = await runOrphanReplySweep();

    expect(summary.excluded).toBe(2);
    expect(judgmentMock).not.toHaveBeenCalled();
  });

  it("a dry run judges but writes and promotes nothing", async () => {
    database([STACY_ROW]);
    judgmentMock.mockResolvedValue({
      model: "m",
      answers: {
        orphan_reply: { type: "choice", choice: "lead_1", confidence: 0.9, probabilities: { lead_1: 0.94, none: 0.06 } },
      },
      usage: { inputTokens: 900, outputTokens: 0 },
    });

    const summary = await runOrphanReplySweep({ dryRun: true });

    expect(summary.recovered).toHaveLength(1);
    expect(updates()).toEqual([]);
    expect(promoteEventMock).not.toHaveBeenCalled();
    expect(actOnInboundReplyMock).not.toHaveBeenCalled();
  });

  it("a failed judgment leaves the row unjudged so the next run offers it again", async () => {
    database([STACY_ROW]);
    judgmentMock.mockRejectedValue(new Error("chat-service 502"));

    const summary = await runOrphanReplySweep();

    expect(summary.failed).toBe(1);
    expect(updates()).toEqual([]);
  });
});
