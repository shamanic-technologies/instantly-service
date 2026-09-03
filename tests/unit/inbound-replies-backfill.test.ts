/**
 * The inbound-replies backfill.
 *
 * The load-bearing property under test is the ASYMMETRY: an autoresponder must
 * leave the sequence running, a human reply must stop it on both sides, and an
 * answer we cannot trust must do nothing at all.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";

const mockPromoteEvent = vi.fn();
const mockQualifyReply = vi.fn();
const mockStopLeadSequence = vi.fn();
const mockDbExecute = vi.fn();

vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockDbExecute(...a) } }));
vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...a: unknown[]) => mockPromoteEvent(...a),
}));
vi.mock("../../src/lib/self-send/qualify-reply", () => ({
  qualifyReply: (...a: unknown[]) => mockQualifyReply(...a),
}));
vi.mock("../../src/lib/stop-lead-sequence", () => ({
  stopLeadSequence: (...a: unknown[]) => mockStopLeadSequence(...a),
}));

import {
  backfillInboundReplies,
  looksLikeAutoresponderSubject,
  promoteCandidate,
  type InboundCandidate,
} from "../../src/lib/inbound-replies-backfill";

/** node-postgres returns a QueryResult OBJECT, never a bare array. */
function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

function candidate(over: Partial<InboundCandidate> = {}): InboundCandidate {
  return {
    bronzeRowId: "bronze-1",
    instantlyCampaignId: "e1e216ca-635a-4682-92be-f5057f8224ea",
    leadEmail: "jason@uhmedical.com",
    orgId: "org-1",
    accountEmail: "kevin.lourd@marketingagency.network",
    subject: "Re: dinner talks",
    body: "I would be interested. Can you send me the costs?",
    receivedAt: new Date("2026-09-01T10:00:00Z"),
    campaignActive: true,
    ...over,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockStopLeadSequence.mockResolvedValue(true);
  mockPromoteEvent.mockResolvedValue({ promoted: true, silverEventId: "e1" });
});

describe("looksLikeAutoresponderSubject", () => {
  it.each([
    "Automatic reply: Purair Hawaii",
    "Out of Office",
    "Re: Automatic reply: outbound efficiency",
    "Risposta automatica: outbound",
    "Réponse automatique : votre message",
    "Automatische Antwort: Anfrage",
  ])("recognises %j", (subject) => {
    expect(looksLikeAutoresponderSubject(subject)).toBe(true);
  });

  it.each([
    "Re: shockwave device ROI benchmarks",
    "Re: your automatic invoice reminder",
    "I would be interested",
    null,
  ])("does NOT claim %j is an autoresponder", (subject) => {
    expect(looksLikeAutoresponderSubject(subject)).toBe(false);
  });
});

describe("promoteCandidate — an autoresponder never stops a sequence", () => {
  it("routes an out-of-office subject to auto_reply_received without an LLM call", async () => {
    const result = await promoteCandidate(
      candidate({ subject: "Automatic reply: dinner talks" }),
    );

    expect(mockQualifyReply).not.toHaveBeenCalled();
    expect(result).toEqual({ kind: "auto", stopped: false });
  });

  it("promotes auto_reply_received and lead_out_of_office, and NOTHING else", async () => {
    await promoteCandidate(candidate({ subject: "Out of Office" }));

    const promoted = mockPromoteEvent.mock.calls.map((c) => c[0].eventType);
    expect(promoted).toEqual(["auto_reply_received", "lead_out_of_office"]);
    expect(promoted).not.toContain("reply_received");
  });

  it("never asks the sender to stop", async () => {
    await promoteCandidate(candidate({ subject: "Out of Office" }));
    expect(mockStopLeadSequence).not.toHaveBeenCalled();
  });

  it("also honours the classifier when the subject looks ordinary", async () => {
    // Mark Hyman's canned "protect deep-focus time" reply carries no
    // autoresponder subject at all — only the classifier catches it.
    mockQualifyReply.mockResolvedValue("lead_out_of_office");

    const result = await promoteCandidate(
      candidate({ subject: "Re: shockwave device ROI benchmarks" }),
    );

    expect(result.stopped).toBe(false);
    expect(mockPromoteEvent.mock.calls.map((c) => c[0].eventType)).not.toContain(
      "reply_received",
    );
  });
});

describe("promoteCandidate — a human reply stops the sequence on both sides", () => {
  it("promotes reply_received AND the kind", async () => {
    mockQualifyReply.mockResolvedValue("lead_interested");

    const result = await promoteCandidate(candidate());

    expect(mockPromoteEvent.mock.calls.map((c) => c[0].eventType)).toEqual([
      "reply_received",
      "lead_interested",
    ]);
    expect(result.kind).toBe("reply");
  });

  it("tags a NEGATIVE reply with its kind too, not only the positives", async () => {
    mockQualifyReply.mockResolvedValue("lead_not_interested");

    await promoteCandidate(candidate());

    expect(mockPromoteEvent.mock.calls.map((c) => c[0].eventType)).toContain(
      "lead_not_interested",
    );
  });

  it("asks the sender to stop, because Instantly never saw this reply", async () => {
    mockQualifyReply.mockResolvedValue("lead_interested");

    const result = await promoteCandidate(candidate());

    expect(mockStopLeadSequence).toHaveBeenCalledWith(
      expect.objectContaining({
        orgId: "org-1",
        instantlyCampaignId: "e1e216ca-635a-4682-92be-f5057f8224ea",
        leadEmail: "jason@uhmedical.com",
      }),
    );
    expect(result.stopped).toBe(true);
  });

  it("still records the reply when the pause fails — the fact is not the side effect", async () => {
    mockQualifyReply.mockResolvedValue("lead_interested");
    mockStopLeadSequence.mockResolvedValue(false);

    const result = await promoteCandidate(candidate());

    expect(result.kind).toBe("reply");
    expect(result.stopped).toBe(false);
  });

  it("skips the pause for a campaign with no org — nothing to authorize with", async () => {
    mockQualifyReply.mockResolvedValue("lead_interested");

    await promoteCandidate(candidate({ orgId: null }));

    expect(mockStopLeadSequence).not.toHaveBeenCalled();
  });
});

describe("promoteCandidate — an unusable classification promotes NOTHING", () => {
  it("writes no event and stops no sequence", async () => {
    mockQualifyReply.mockResolvedValue(null);

    const result = await promoteCandidate(candidate());

    expect(result).toEqual({ kind: "unclassifiable", stopped: false });
    expect(mockPromoteEvent).not.toHaveBeenCalled();
    expect(mockStopLeadSequence).not.toHaveBeenCalled();
  });

  it("never defaults to neutral", async () => {
    mockQualifyReply.mockResolvedValue(null);

    await promoteCandidate(candidate());

    expect(mockPromoteEvent.mock.calls.map((c) => c[0]?.eventType)).not.toContain(
      "lead_neutral",
    );
  });
});

describe("backfillInboundReplies — dry run is the default and writes nothing", () => {
  it("reports the plan without promoting or classifying", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        {
          bronzeRowId: "b1",
          instantlyCampaignId: "c1",
          leadEmail: "a@x.com",
          orgId: "org-1",
          accountEmail: "s@y.com",
          subject: "Re: hi",
          body: "sure",
          receivedAt: "2026-09-01T10:00:00Z",
          campaignActive: true,
        },
      ]),
    );

    const summary = await backfillInboundReplies();

    expect(summary.candidates).toBe(1);
    expect(mockPromoteEvent).not.toHaveBeenCalled();
    expect(mockQualifyReply).not.toHaveBeenCalled();
  });

  it("an empty backlog reports zero rather than failing", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));

    const summary = await backfillInboundReplies({ dryRun: true });

    expect(summary.candidates).toBe(0);
    expect(summary.truncated).toBe(false);
  });
});

describe("backfillInboundReplies — the sweep totals its buckets honestly", () => {
  const rows = [
    {
      bronzeRowId: "b1",
      instantlyCampaignId: "c1",
      leadEmail: "a@x.com",
      orgId: "org-1",
      accountEmail: "s@y.com",
      subject: "Out of Office",
      body: "away",
      receivedAt: "2026-09-01T10:00:00Z",
      campaignActive: true,
    },
    {
      bronzeRowId: "b2",
      instantlyCampaignId: "c2",
      leadEmail: "b@x.com",
      orgId: "org-1",
      accountEmail: "s@y.com",
      subject: "Re: hi",
      body: "interested!",
      receivedAt: "2026-09-01T11:00:00Z",
      campaignActive: true,
    },
    {
      bronzeRowId: "b3",
      instantlyCampaignId: "c3",
      leadEmail: "c@x.com",
      orgId: "org-1",
      accountEmail: "s@y.com",
      subject: "Re: hi",
      body: "???",
      receivedAt: "2026-09-01T12:00:00Z",
      campaignActive: false,
    },
  ];

  it("counts auto-replies, replies and unclassifiable separately", async () => {
    mockDbExecute.mockResolvedValue(pgResult(rows));
    mockQualifyReply
      .mockResolvedValueOnce("lead_interested")
      .mockResolvedValueOnce(null);

    const summary = await backfillInboundReplies({ dryRun: false });

    expect(summary).toMatchObject({
      candidates: 3,
      autoReplies: 1,
      replies: 1,
      unclassifiable: 1,
      sequencesStopped: 1,
      failed: 0,
    });
  });

  it("one failing candidate is counted, not fatal to the rest", async () => {
    mockDbExecute.mockResolvedValue(pgResult(rows));
    mockQualifyReply.mockRejectedValueOnce(new Error("chat-service 502"));
    mockQualifyReply.mockResolvedValueOnce("lead_neutral");

    const summary = await backfillInboundReplies({ dryRun: false });

    expect(summary.failed).toBe(1);
    expect(summary.autoReplies).toBe(1);
  });

  it("reports `truncated` when a limit filled the batch", async () => {
    mockDbExecute.mockResolvedValue(pgResult(rows.slice(0, 2)));

    const summary = await backfillInboundReplies({ dryRun: true, limit: 2 });

    expect(summary.truncated).toBe(true);
  });

  it("is idempotent: a second run over an emptied backlog reports zero", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));

    const summary = await backfillInboundReplies({ dryRun: false });

    expect(summary).toMatchObject({ candidates: 0, replies: 0, autoReplies: 0 });
    expect(mockPromoteEvent).not.toHaveBeenCalled();
  });
});

describe("backfillInboundReplies — the candidate SQL", () => {
  it("excludes campaigns that already carry EITHER reply event", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));
    await backfillInboundReplies({ dryRun: true });

    const text = JSON.stringify(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("reply_received");
    expect(text).toContain("auto_reply_received");
  });

  it("reads only INBOUND rows and skips self-send sequences", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));
    await backfillInboundReplies({ dryRun: true });

    const text = JSON.stringify(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("ue_type");
    expect(text).toContain("self:%");
  });

  it("orders live sequences first — the slice still emailing people", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));
    await backfillInboundReplies({ dryRun: true });

    const text = JSON.stringify(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("ORDER BY");
    expect(text).toContain("active");
  });
});
