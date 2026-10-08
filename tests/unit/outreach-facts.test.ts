/**
 * The outreach fact feed (lib/outreach-facts) and the reply distinctions it
 * serves (lib/reply-kind replyKindDistinctions, lib/reply-judgments).
 *
 * The SQL was validated against production data in a rolled-back transaction;
 * these tests pin the rules it encodes and the pure planning around it.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

const mockPlatformJudgment = vi.fn();
vi.mock("../../src/lib/chat-client", () => ({
  platformJudgment: (...a: unknown[]) => mockPlatformJudgment(...a),
}));

import {
  eventFactsInsertSql,
  eventWithdrawalsInsertSql,
  planReplyFacts,
  replyContentHash,
  toOutreachFact,
} from "../../src/lib/outreach-facts";
import { judgeReply, questionsForKind, NO_TEXT } from "../../src/lib/reply-judgments";
import { REPLY_KINDS, replyKindDistinctions, replyKindFacts } from "../../src/lib/reply-kind";
import type { ReplyVerdictView } from "../../src/lib/reply-verdicts";

function sqlText(obj: unknown): string {
  if (typeof obj === "string") return obj;
  if (obj == null) return "";
  if (Array.isArray(obj)) return obj.map(sqlText).join("");
  if (typeof obj === "object") {
    const o = obj as Record<string, unknown>;
    if (Array.isArray(o.value)) return o.value.join("");
    if (Array.isArray(o.queryChunks)) return sqlText(o.queryChunks);
    return Object.values(o).map(sqlText).join("");
  }
  return "";
}

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

beforeEach(() => {
  vi.resetAllMocks();
  mockDbExecute.mockResolvedValue(pgResult([]));
});

function view(over: Partial<ReplyVerdictView> = {}, kind: string | null = "lead_not_interested"): ReplyVerdictView {
  return {
    replyId: "imap:1",
    leadEmail: "christina@wellconnectedchiro.com",
    instantlyCampaignId: "self:65e6",
    campaignId: "camp-1",
    brandIds: ["75d7e3e8-6926-4f85-a557-976895400666"],
    transport: "smtp",
    fromEmail: "christina@wellconnectedchiro.com",
    subject: "Re: hello",
    receivedAt: "2026-10-07T22:13:14.000Z",
    verdict:
      kind === null
        ? null
        : {
            kind,
            classification: "negative",
            producerType: "model",
            producer: "deepseek-flash",
            attribution: "exact",
            confidence: null,
            decidedAt: "2026-10-07T22:14:00.000Z",
            ...replyKindFacts(kind),
            ...replyKindDistinctions(kind),
          },
    verdictCount: 1,
    judgments: { proposalType: null, question: { value: "none", confidence: 0.98 } },
    escalation: null,
    ...over,
  };
}

describe("reply distinctions (own words, never a kind name)", () => {
  it("tells apart what one coarse flag lumps together", () => {
    // notOurTarget: wrong contact vs left the role vs already a customer.
    expect(replyKindDistinctions("lead_wrong_person").notOurTargetReason).toBe("wrong_contact");
    expect(replyKindDistinctions("lead_changed_job").notOurTargetReason).toBe("left_role");
    expect(replyKindDistinctions("lead_already_customer").notOurTargetReason).toBe("already_customer");
    // handedToPerson: referral vs an unrelated proposal.
    expect(replyKindDistinctions("lead_referral").handoffReason).toBe("referral");
    expect(replyKindDistinctions("lead_off_topic").handoffReason).toBe("unrelated_proposal");
    // positive: interest vs a request for information vs a meeting.
    expect(replyKindDistinctions("lead_interested").positiveSignal).toBe("interest");
    expect(replyKindDistinctions("lead_info_requested").positiveSignal).toBe("information_request");
    expect(replyKindDistinctions("lead_meeting_requested").positiveSignal).toBe("meeting_request");
    // a plain no is declined, recyclable, and nothing else.
    expect(replyKindDistinctions("lead_not_interested")).toEqual({
      positiveSignal: null,
      declinedOffer: true,
      notOurTargetReason: null,
      handoffReason: null,
    });
  });

  it("agrees with the coarse flags for every kind", () => {
    for (const kind of REPLY_KINDS) {
      const facts = replyKindFacts(kind);
      const d = replyKindDistinctions(kind);
      expect(d.notOurTargetReason !== null, kind).toBe(facts.notOurTarget);
      expect(d.handoffReason !== null, kind).toBe(facts.handedToPerson);
      for (const value of [d.positiveSignal, d.notOurTargetReason, d.handoffReason]) {
        // Own words: no served value is a reply kind name.
        expect((REPLY_KINDS as readonly string[]).includes(String(value)), kind).toBe(false);
      }
    }
  });
});

describe("reply judgments (Jev, once per reply)", () => {
  it("asks the proposal subtype only of an unrelated proposal, and nothing of an autoresponder", () => {
    expect(questionsForKind("lead_off_topic")).toEqual(["proposal_type", "question"]);
    expect(questionsForKind("lead_info_requested")).toEqual(["question"]);
    expect(questionsForKind("lead_out_of_office")).toEqual([]);
    expect(questionsForKind("auto_reply_received")).toEqual([]);
    expect(questionsForKind(null)).toEqual([]);
  });

  it("asks every missing question in ONE call and stores each answer", async () => {
    mockPlatformJudgment.mockResolvedValue({
      model: "m",
      usage: { inputTokens: 120, outputTokens: 0 },
      answers: {
        proposal_type: { type: "choice", choice: "hiring", confidence: 0.9, probabilities: { hiring: 0.95 } },
        question: { type: "choice", choice: "none", confidence: 0.8, probabilities: { none: 0.9 } },
      },
    });
    const stored = await judgeReply({
      replyId: "imap:9",
      subject: "Re: hello",
      body: "Are you hiring? I'd love to join your team\n\nOn Mon, Bria wrote:\n> our offer",
      missing: ["proposal_type", "question"],
    });
    expect(stored).toBe(2);
    expect(mockPlatformJudgment).toHaveBeenCalledTimes(1);
    const call = mockPlatformJudgment.mock.calls[0][0];
    expect(Object.keys(call.questions)).toEqual(["proposal_type", "question"]);
    // Their words only, under the subject; our quoted email is stripped.
    expect(call.state).toContain("Are you hiring?");
    expect(call.state).not.toContain("our offer");
    const inserts = mockDbExecute.mock.calls.map((c) => sqlText(c[0]));
    expect(inserts.every((q) => q.includes("ON CONFLICT (reply_id, question) DO NOTHING"))).toBe(true);
  });

  it("stores nothing for an answer outside the vocabulary, and throws", async () => {
    mockPlatformJudgment.mockResolvedValue({
      model: "m",
      usage: { inputTokens: 1, outputTokens: 0 },
      answers: { question: { type: "choice", choice: "maybe", confidence: 1, probabilities: {} } },
    });
    await expect(
      judgeReply({ replyId: "r", subject: null, body: "what?", missing: ["question"] }),
    ).rejects.toThrow(/unreadable answer/);
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("records a reply with no words once, without calling the engine", async () => {
    const stored = await judgeReply({ replyId: "r", subject: null, body: "> only quoted", missing: ["question"] });
    expect(stored).toBe(0);
    expect(mockPlatformJudgment).not.toHaveBeenCalled();
    expect(mockDbExecute.mock.calls[0][0].queryChunks.map(sqlText).join("")).toContain("INSERT INTO reply_judgments");
    expect(JSON.stringify(mockDbExecute.mock.calls[0][0])).toContain(NO_TEXT);
  });
});

describe("event facts", () => {
  it("emits each REAL event once, oldest first, with the clicked URL from our own tracker", () => {
    const q = sqlText(eventFactsInsertSql(null));
    expect(q).toContain("e.inferred = false");
    expect(q).toContain("ON CONFLICT (subject_key)");
    expect(q).toContain("DO NOTHING");
    expect(q).toContain("'ievt:' || e.id");
    expect(q).toContain("h.payload->>'url'");
    expect(q).toContain("ORDER BY e.timestamp, e.id");
    // A withdrawn recorded opt-out never enters; reserving sentinels never do.
    expect(q).toContain("e.withdrawn_at IS NULL");
    expect(q).toContain("NOT LIKE 'reserving:%'");
    // No window = the whole history.
    expect(q).not.toContain("e.created_at >");
  });

  it("scans by INSERTION time when windowed", () => {
    expect(sqlText(eventFactsInsertSql(2))).toContain("e.created_at > now()");
  });

  it("states first vs follow-up from the step, else from the order on the thread", () => {
    const q = sqlText(eventFactsInsertSql(null));
    expect(q).toContain("WHEN e.step = 1 THEN 'first' ELSE 'followup'");
    expect(q).toContain("'positionBasis'");
    expect(q).toContain("p.timestamp < e.timestamp");
    // An unstepped poll copy of a stepped send is the same email, skipped.
    expect(q).toContain("abs(extract(epoch FROM s.timestamp - e.timestamp))");
  });

  it("withdraws a click, bounce or opt-out whose source is gone, once", () => {
    const q = sqlText(eventWithdrawalsInsertSql());
    expect(q).toContain("'withdrawn'");
    expect(q).toContain("e.id IS NULL OR e.withdrawn_at IS NOT NULL");
    expect(q).toContain("NOT EXISTS (SELECT 1 FROM outreach_facts w WHERE w.supersedes_seq = f.seq)");
  });
});

describe("reply facts: corrections are new facts", () => {
  const latest = (entries: [string, { seq: number; type: string; hash: string | null }][]) =>
    new Map(entries.map(([k, v]) => [k, { ...v, row: {} }]));

  it("emits a new reply once, then nothing while it says the same", () => {
    const v = view();
    const c = { view: v, orgId: "org", hash: replyContentHash(v), held: false };
    expect(planReplyFacts([c], new Map()).emit).toEqual([{ candidate: c, supersedesSeq: null }]);
    expect(planReplyFacts([c], latest([["reply:imap:1", { seq: 7, type: "reply", hash: c.hash }]])).emit).toEqual([]);
  });

  it("supersedes when the verdict, a judgment or the escalation changes", () => {
    const before = view();
    const after = view({ escalation: { escalatedAt: "2026-10-08T01:00:00.000Z", handedTo: "agency" } });
    expect(replyContentHash(after)).not.toBe(replyContentHash(before));
    const c = { view: after, orgId: "org", hash: replyContentHash(after), held: false };
    const plan = planReplyFacts([c], latest([["reply:imap:1", { seq: 7, type: "reply", hash: replyContentHash(before) }]]));
    expect(plan.emit[0].supersedesSeq).toBe(7);
  });

  it("does not re-emit on bookkeeping noise (a re-poll restating the same verdict)", () => {
    const a = view();
    const b = view({ verdictCount: 3 });
    b.verdict = { ...b.verdict!, decidedAt: "2026-10-08T09:00:00.000Z", confidence: 0.5 };
    expect(replyContentHash(b)).toBe(replyContentHash(a));
  });

  it("holds a reply still owed its verdict or a judgment, and withdraws one that vanished", () => {
    const held = { view: view({}, null), orgId: "org", hash: "h", held: true };
    const plan = planReplyFacts([held], latest([["reply:manual:1", { seq: 3, type: "reply", hash: "x" }]]));
    expect(plan.emit).toEqual([]);
    expect(plan.held).toBe(1);
    expect(plan.withdraw.map((w) => w.seq)).toEqual([3]);
  });

  it("restates a reply that comes back after a withdrawal as a first statement", () => {
    const v = view();
    const c = { view: v, orgId: "org", hash: replyContentHash(v), held: false };
    const plan = planReplyFacts([c], latest([["reply:imap:1", { seq: 9, type: "withdrawn", hash: null }]]));
    expect(plan.emit[0].supersedesSeq).toBeNull();
    expect(plan.withdraw).toEqual([]);
  });
});

describe("the served fact", () => {
  const base = {
    seq: 42,
    subject_key: "ievt:e1",
    supersedes_seq: null,
    occurred_at: new Date("2026-10-01T10:00:00Z"),
    recorded_at: new Date("2026-10-08T10:00:00Z"),
    lead_email: "christina@wellconnectedchiro.com",
    org_id: "org",
    campaign_id: "camp",
    instantly_campaign_id: "self:1",
    brand_ids: ["b"],
    transport: "smtp",
  };

  it("fills exactly the object matching its type", () => {
    const sent = toOutreachFact({
      ...base,
      type: "email_sent",
      payload: { step: 3, position: "followup", positionBasis: "step", accountEmail: "bria@x.com" },
    });
    expect(sent.seq).toBe("42");
    expect(sent.send).toEqual({ step: 3, position: "followup", positionBasis: "step", accountEmail: "bria@x.com" });
    expect([sent.open, sent.click, sent.bounce, sent.unsubscribe, sent.reply, sent.withdrawal]).toEqual([
      null, null, null, null, null, null,
    ]);

    const click = toOutreachFact({ ...base, type: "link_clicked", payload: { step: 1, url: "https://brand.com/?a=1" } });
    expect(click.click).toEqual({ step: 1, url: "https://brand.com/?a=1" });

    const w = toOutreachFact({
      ...base,
      type: "withdrawn",
      supersedes_seq: 12,
      payload: { withdrawnType: "link_clicked", reason: "source_removed" },
    });
    expect(w.supersedesSeq).toBe("12");
    expect(w.withdrawal).toEqual({ withdrawnSeq: "12", withdrawnType: "link_clicked", reason: "source_removed" });
  });

  it("serves a reply with its verdict and distinctions (Christina: negative, declined)", () => {
    const fact = toOutreachFact({ ...base, subject_key: "reply:imap:1", type: "reply", payload: view() });
    expect(fact.reply?.verdict?.classification).toBe("negative");
    expect(fact.reply?.verdict?.declinedOffer).toBe(true);
    expect(fact.reply?.judgments.question?.value).toBe("none");
  });
});
