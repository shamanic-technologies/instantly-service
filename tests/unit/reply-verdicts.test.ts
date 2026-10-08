/**
 * One current verdict per real reply (lib/reply-verdicts).
 *
 * The SQL itself was validated against production data in a rolled-back
 * transaction (468 replies, Elena's two messages as two verdicts); these tests
 * pin the rules the SQL encodes, so a later edit cannot quietly drop one.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

const mockQualifyReply = vi.fn();
vi.mock("../../src/lib/self-send/qualify-reply", () => ({
  qualifyReply: (...a: unknown[]) => mockQualifyReply(...a),
}));

const mockPromoteEvent = vi.fn();
vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...a: unknown[]) => mockPromoteEvent(...a),
}));

import {
  backfillReplyVerdicts,
  readReplyVerdicts,
  syncReplyVerdicts,
} from "../../src/lib/reply-verdicts";

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

describe("the projection", () => {
  it("mirrors kind events into bronze idempotently, keyed on the event id", async () => {
    await syncReplyVerdicts({ sinceDays: null });
    const ingest = sqlText(mockDbExecute.mock.calls[0][0]);
    expect(ingest).toContain("INSERT INTO reply_verdicts_raw");
    expect(ingest).toContain("ON CONFLICT (source_event_id) DO NOTHING");
    expect(ingest).toContain("e.inferred = false");
    // A person's statement is a human verdict.
    expect(ingest).toContain("WHEN e.source = 'manual' THEN 'human'");
  });

  it("counts only REAL replies: not staff, not our own mailboxes, not a mail server", async () => {
    await syncReplyVerdicts({ sinceDays: null });
    const upsert = sqlText(mockDbExecute.mock.calls[1][0]);
    expect(upsert).toContain("INSERT INTO replies");
    expect(upsert).toContain("split_part(coalesce(");
    expect(upsert).toContain("FROM instantly_accounts a");
    expect(upsert).toContain("'postmaster', 'mailer-daemon'");
    expect(upsert).toContain("m.kind IN ('reply', 'auto_reply')");
  });

  it("makes a reply of a verdict whose message was never mirrored, from ANY producer", async () => {
    await syncReplyVerdicts({ sinceDays: null });
    const upsert = sqlText(mockDbExecute.mock.calls[2][0]);
    expect(upsert).toContain("INSERT INTO replies");
    // A person's statement keeps its manual id; Instantly's is keyed on the event.
    expect(upsert).toContain("'manual:' || f.source_row_id");
    expect(upsert).toContain("'ievt:' || f.id");
    expect(upsert).toContain("'instantly_events'");
    // Instantly's own verdicts are stated too, with their own transport.
    expect(upsert).toContain("ELSE 'instantly' END AS transport");
    expect(upsert).toContain("e.withdrawn_at IS NULL");
    expect(upsert).toContain("e.inferred = false");
    // One per thread: the earliest standing verdict.
    expect(upsert).toContain("DISTINCT ON (e.campaign_id)");
    expect(upsert).toContain("ORDER BY e.campaign_id, e.timestamp, e.id");
    // A thread whose campaign row is gone still gets its reply; the org is looked up elsewhere.
    expect(upsert).toContain("LEFT JOIN instantly_campaigns c");
    expect(upsert).toContain("FROM instantly_campaigns_config_raw k");
    expect(upsert).toContain("COALESCE(c.lead_email, f.lead_email)");
    // Only where no MIRRORED reply precedes it (same skew as attribution).
    expect(upsert).toContain("r.source_table IN (");
    expect(upsert).toContain("+ interval '10 minutes'");
    const prune = sqlText(mockDbExecute.mock.calls[3][0]);
    expect(prune).toContain("DELETE FROM replies r");
    expect(mockPromoteEvent).not.toHaveBeenCalled();
  });

  it("an Instantly verdict attributes exactly to the reply it stated", async () => {
    await syncReplyVerdicts({ sinceDays: null });
    const recompute = sqlText(mockDbExecute.mock.calls[5][0]);
    expect(recompute).toContain("r.source_table = 'instantly_events' AND r.source_row_id = v.source_event_id");
  });

  it("the backfill classifier never tries to read a hand-recorded reply", async () => {
    await backfillReplyVerdicts({ sinceDays: null });
    const select = mockDbExecute.mock.calls.map((c) => sqlText(c[0])).find((t) => t.includes("r.current_kind IS NULL AND"));
    expect(select).toContain("r.source_table IN (");
  });

  it("attributes exactly first, then to the latest reply before the verdict, and drops withdrawn statements", async () => {
    await syncReplyVerdicts({ sinceDays: null });
    const recompute = sqlText(mockDbExecute.mock.calls[5][0]);
    expect(recompute).toContain("'exact' AS attribution");
    expect(recompute).toContain("'latest_before'");
    expect(recompute).toContain("r.received_at <= v.decided_at + interval '10 minutes'");
    expect(recompute).toContain("ORDER BY rank, received_at DESC");
    expect(recompute).toContain("ev.withdrawn_at IS NULL");
  });

  it("a person's statement beats everyone, then the most recent", async () => {
    await syncReplyVerdicts({ sinceDays: null });
    const recompute = sqlText(mockDbExecute.mock.calls[5][0]);
    expect(recompute).toContain("(producer_type = 'human') DESC, decided_at DESC");
  });

  it("clears the current verdict of a reply whose verdicts all went away", async () => {
    await syncReplyVerdicts({ sinceDays: null });
    const reset = sqlText(mockDbExecute.mock.calls[4][0]);
    expect(reset).toContain("current_verdict_id = NULL");
    expect(reset).toContain("NOT EXISTS (SELECT 1 FROM attributed a WHERE a.reply_id = r.id)");
  });
});

describe("the backfill", () => {
  function queueUnjudged(rows: Record<string, unknown>[]) {
    // sync: ingest, upsert, hand-recorded upsert + prune, reset, recompute; then the candidate select.
    mockDbExecute
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult(rows));
  }

  it("records a verdict in BRONZE ONLY — no silver event, so no side effect fires", async () => {
    queueUnjudged([
      {
        id: "ie:abc",
        instantly_campaign_id: "ic-1",
        lead_email: "jamie@kinetikchaindenver.com",
        subject: "Re: x",
        body: "Not w/o an estimate of price.",
      },
    ]);
    mockQualifyReply.mockResolvedValue("lead_not_interested");

    const summary = await backfillReplyVerdicts({ sinceDays: null });

    expect(summary).toMatchObject({ candidates: 1, classified: 1 });
    expect(mockPromoteEvent).not.toHaveBeenCalled();
    const insert = mockDbExecute.mock.calls.map((c) => sqlText(c[0])).find((t) => t.includes("'backfill_classifier'"));
    expect(insert).toBeDefined();
    expect(mockQualifyReply).toHaveBeenCalledWith(
      "Not w/o an estimate of price.",
      expect.objectContaining({ subject: "Re: x", source: "reply_verdict_backfill" }),
    );
  });

  it("never invents a verdict: an unusable classification or an empty body records nothing", async () => {
    queueUnjudged([
      { id: "ie:1", instantly_campaign_id: "ic-1", lead_email: "a@b.com", subject: null, body: "hmm" },
      { id: "ie:2", instantly_campaign_id: "ic-2", lead_email: "c@d.com", subject: null, body: "" },
    ]);
    mockQualifyReply.mockResolvedValue(null);

    const summary = await backfillReplyVerdicts({ sinceDays: null });

    expect(summary).toMatchObject({ candidates: 2, classified: 0, unqualified: 1, noBody: 1 });
    expect(mockDbExecute.mock.calls.some((c) => sqlText(c[0]).includes("'backfill_classifier'"))).toBe(false);
  });
});

describe("the read", () => {
  it("is org-scoped, matches the address case-insensitively, and maps the current verdict", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        {
          id: "ie:1",
          lead_email: "elena.staeheli@biopartner.ch",
          instantly_campaign_id: "fbde0b33",
          campaign_id: "38ba8069",
          brand_ids: ["f2408cfb"],
          transport: "instantly",
          from_email: "elena.staeheli@biopartner.ch",
          subject: "Re: x",
          received_at: new Date("2026-09-28T04:52:00Z"),
          current_kind: "lead_referral",
          current_classification: "neutral",
          current_producer_type: "model",
          current_producer: "deepseek-flash",
          current_attribution: "exact",
          current_confidence: null,
          current_decided_at: new Date("2026-09-29T05:00:00Z"),
          verdict_count: 1,
        },
      ]),
    );

    const out = await readReplyVerdicts({ orgId: "org-1", emails: ["Elena.Staeheli@Biopartner.ch"] });

    const text = sqlText(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("r.org_id = ");
    expect(text).toContain("lower(r.lead_email) IN (");
    expect(out[0]).toMatchObject({
      replyId: "ie:1",
      campaignId: "38ba8069",
      brandIds: ["f2408cfb"],
      receivedAt: "2026-09-28T04:52:00.000Z",
      verdict: {
        kind: "lead_referral",
        classification: "neutral",
        producerType: "model",
        automatedAnswer: false,
        stopRequested: false,
        notOurTarget: false,
        handedToPerson: true,
      },
    });
  });

  it("adds the distinctions, the Jev judgments and the escalation (additive)", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        {
          id: "imap:7",
          lead_email: "p@x.com",
          instantly_campaign_id: "self:1",
          campaign_id: "c",
          brand_ids: ["b"],
          transport: "smtp",
          from_email: "p@x.com",
          subject: "Re: x",
          received_at: new Date("2026-10-01T10:00:00Z"),
          current_kind: "lead_off_topic",
          current_classification: "neutral",
          current_producer_type: "model",
          current_producer: "deepseek-flash",
          current_attribution: "exact",
          current_confidence: null,
          current_decided_at: new Date("2026-10-01T10:01:00Z"),
          verdict_count: 1,
          esc_at: new Date("2026-10-01T11:00:00Z"),
          esc_handed_to: "agency",
        },
      ]),
    );
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        { reply_id: "imap:7", question: "proposal_type", choice: "partnership", confidence: 0.91 },
        { reply_id: "imap:7", question: "question", choice: "none", confidence: 0.7 },
      ]),
    );

    const [out] = await readReplyVerdicts({ orgId: "org-1", emails: ["p@x.com"] });

    expect(sqlText(mockDbExecute.mock.calls[0][0])).toContain("LEFT JOIN LATERAL");
    expect(out.verdict).toMatchObject({ handedToPerson: true, handoffReason: "unrelated_proposal", positiveSignal: null });
    expect(out.judgments).toEqual({
      proposalType: { value: "partnership", confidence: 0.91 },
      question: { value: "none", confidence: 0.7 },
    });
    expect(out.escalation).toEqual({ escalatedAt: "2026-10-01T11:00:00.000Z", handedTo: "agency" });
  });

  it.each([
    ["lead_out_of_office", { automatedAnswer: true, stopRequested: false, notOurTarget: false, handedToPerson: false }],
    ["auto_reply_received", { automatedAnswer: true, stopRequested: false, notOurTarget: false, handedToPerson: false }],
    ["lead_opt_out_requested", { automatedAnswer: false, stopRequested: true, notOurTarget: false, handedToPerson: false }],
    ["lead_wrong_person", { automatedAnswer: false, stopRequested: false, notOurTarget: true, handedToPerson: false }],
    ["lead_changed_job", { automatedAnswer: false, stopRequested: false, notOurTarget: true, handedToPerson: false }],
    // A plain no stays recyclable: not a disqualification.
    ["lead_not_interested", { automatedAnswer: false, stopRequested: false, notOurTarget: false, handedToPerson: false }],
    // A hand-over is not a plain neutral reply.
    ["lead_referral", { automatedAnswer: false, stopRequested: false, notOurTarget: false, handedToPerson: true }],
    ["lead_off_topic", { automatedAnswer: false, stopRequested: false, notOurTarget: false, handedToPerson: true }],
    ["lead_neutral", { automatedAnswer: false, stopRequested: false, notOurTarget: false, handedToPerson: false }],
    ["lead_interested", { automatedAnswer: false, stopRequested: false, notOurTarget: false, handedToPerson: false }],
  ])("states the facts of a %s verdict", async (kind, facts) => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        {
          id: "manual:q1",
          lead_email: "jason@uhmedical.com",
          instantly_campaign_id: "e1e216ca",
          campaign_id: "c1",
          brand_ids: [],
          transport: "manual",
          from_email: null,
          subject: null,
          received_at: new Date("2026-09-03T13:21:37Z"),
          current_kind: kind,
          current_classification: "neutral",
          current_producer_type: "human",
          current_producer: "manual",
          current_attribution: "exact",
          current_confidence: null,
          current_decided_at: new Date("2026-09-03T13:21:37Z"),
          verdict_count: 1,
        },
      ]),
    );
    const [row] = await readReplyVerdicts({ orgId: "org-1", emails: ["jason@uhmedical.com"] });
    expect(row.verdict).toMatchObject(facts);
  });

  it("returns nothing, and queries nothing, for an empty address list", async () => {
    expect(await readReplyVerdicts({ orgId: "org-1", emails: ["  "] })).toEqual([]);
    expect(mockDbExecute).not.toHaveBeenCalled();
  });
});
