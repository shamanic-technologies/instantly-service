import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";

// ─── Mocks ───────────────────────────────────────────────────────────────────
//
// `db.execute` is mocked in the shape node-postgres actually returns — a
// QueryResult OBJECT, never a bare array. The repo has already paid for that
// distinction once: a mock returning an array agreed with the bug rather than
// with the driver, so a whole suite passed against code that could not run.

const mockExecute = vi.fn();

function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));

const mockQualifyReply = vi.fn();
// Passthrough form: only the network call is overridden, so the module's pure
// exports (the label list) stay real and the lockstep test means something.
vi.mock("../../src/lib/self-send/qualify-reply", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  qualifyReply: (...a: unknown[]) => mockQualifyReply(...a),
}));

const mockRecordLeadOptOut = vi.fn();
const mockFindStandingOptOut = vi.fn();
vi.mock("../../src/lib/lead-optouts", () => ({
  recordLeadOptOut: (...a: unknown[]) => mockRecordLeadOptOut(...a),
  findStandingOptOut: (...a: unknown[]) => mockFindStandingOptOut(...a),
}));

import {
  OPT_OUT_REPLY_KIND,
  REPLY_OPT_OUT_STATED_BY,
  fetchLatestMirroredInbound,
  maybeRecordOptOutFromReply,
  recordOptOutFromReply,
} from "../../src/lib/reply-opt-out";
import {
  DISQUALIFYING_REPLY_KINDS,
  REPLY_KIND_CLASSIFICATION,
  REPLY_KINDS,
  SEQUENCE_STOPPING_REPLY_KINDS,
  isDisqualifyingReplyKind,
} from "../../src/lib/reply-kind";
import {
  QUALIFICATION_EVENT_TYPES,
  SYSTEM_PROMPT,
} from "../../src/lib/self-send/qualify-reply";
import { SENTIMENT_EVENT_TYPES } from "../../src/routes/analytics";

const CAMPAIGN = {
  instantlyCampaignId: "camp-1",
  leadEmail: "chad@clinic.com",
  orgId: "org-1",
};

beforeEach(() => {
  vi.resetAllMocks();
  mockExecute.mockResolvedValue(pgResult([]));
  mockFindStandingOptOut.mockResolvedValue(null);
  mockRecordLeadOptOut.mockResolvedValue({
    recorded: true,
    optOut: {},
    campaignsAffected: 2,
    campaignsStopped: 2,
  });
});

// ─── Vocabulary ──────────────────────────────────────────────────────────────

describe("lead_opt_out_requested — the vocabulary", () => {
  it("is a reply kind, classifies negative, and STOPS the sequence", () => {
    expect(REPLY_KINDS).toContain(OPT_OUT_REPLY_KIND);
    expect(REPLY_KIND_CLASSIFICATION[OPT_OUT_REPLY_KIND]).toBe("negative");
    expect(SEQUENCE_STOPPING_REPLY_KINDS.has(OPT_OUT_REPLY_KIND)).toBe(true);
  });

  it("DISQUALIFIES, unlike lead_not_interested which stays recyclable", () => {
    // The whole point of the kind. Seven prod leads who wrote "remove me" were
    // filed `lead_not_interested` — i.e. re-contactable in three months.
    expect(isDisqualifyingReplyKind(OPT_OUT_REPLY_KIND)).toBe(true);
    expect(DISQUALIFYING_REPLY_KINDS.has(OPT_OUT_REPLY_KIND)).toBe(true);
    expect(isDisqualifyingReplyKind("lead_not_interested")).toBe(false);
  });

  it("is a label the classifier can actually emit", () => {
    expect(QUALIFICATION_EVENT_TYPES).toContain(OPT_OUT_REPLY_KIND);
  });

  it("is DELIBERATELY absent from SENTIMENT_EVENT_TYPES — adding it double-counts", () => {
    // An opt-out reply produces the kind AND the `lead_unsubscribed` the recorded
    // opt-out promotes. `repliesDetail.unsubscribe` already sums into
    // `repliesNegative`, so counting the kind there too counts one reply twice.
    expect(SENTIMENT_EVENT_TYPES).not.toContain(OPT_OUT_REPLY_KIND);
  });
});

// ─── Classify + record ───────────────────────────────────────────────────────

describe("recordOptOutFromReply", () => {
  it("records an opt-out on the email_reply channel, naming the classifier", async () => {
    mockQualifyReply.mockResolvedValue(OPT_OUT_REPLY_KIND);

    const result = await recordOptOutFromReply({
      campaign: CAMPAIGN,
      replyText: "Please remove me from your email list.",
      evidence: { source: "mirrored_reply" },
    });

    expect(result).toEqual({ recorded: true, campaignsAffected: 2 });
    expect(mockRecordLeadOptOut).toHaveBeenCalledTimes(1);
    const arg = mockRecordLeadOptOut.mock.calls[0][0];
    expect(arg.orgId).toBe("org-1");
    expect(arg.leadEmail).toBe("chad@clinic.com");
    expect(arg.channel).toBe("email_reply");
    expect(arg.statedBy).toBe(REPLY_OPT_OUT_STATED_BY);
    // The prospect's own sentence is the audit — that is what makes recording a
    // consent fact from a machine classification legitimate at all.
    expect(arg.notes).toContain("remove me");
  });

  it("records NOTHING for any other kind", async () => {
    mockQualifyReply.mockResolvedValue("lead_not_interested");

    const result = await recordOptOutFromReply({
      campaign: CAMPAIGN,
      replyText: "Not for us, thanks.",
      evidence: {},
    });

    expect(result).toEqual({ recorded: false, reason: "not_an_opt_out" });
    expect(mockRecordLeadOptOut).not.toHaveBeenCalled();
  });

  it("records NOTHING on an unusable classification — never defaults to 'no'", async () => {
    mockQualifyReply.mockResolvedValue(null);

    const result = await recordOptOutFromReply({
      campaign: CAMPAIGN,
      replyText: "unsubscribe",
      evidence: {},
    });

    expect(result).toEqual({ recorded: false, reason: "unqualified" });
    expect(mockRecordLeadOptOut).not.toHaveBeenCalled();
  });

  it("takes a caller's classification without paying for a second model call", async () => {
    await recordOptOutFromReply({
      campaign: CAMPAIGN,
      replyText: "unsubscribe",
      qualification: OPT_OUT_REPLY_KIND,
      evidence: {},
    });

    expect(mockQualifyReply).not.toHaveBeenCalled();
    expect(mockRecordLeadOptOut).toHaveBeenCalledTimes(1);
  });

  it("skips a platform send — no org means no consent scope to record against", async () => {
    const result = await recordOptOutFromReply({
      campaign: { ...CAMPAIGN, orgId: null },
      replyText: "remove me",
      evidence: {},
    });

    expect(result).toEqual({ recorded: false, reason: "no_org" });
    expect(mockQualifyReply).not.toHaveBeenCalled();
  });

  it("propagates a classifier failure — the CALLERS swallow, not this", async () => {
    mockQualifyReply.mockRejectedValue(new Error("chat-service down"));

    await expect(
      recordOptOutFromReply({ campaign: CAMPAIGN, replyText: "x", evidence: {} }),
    ).rejects.toThrow("chat-service down");
  });
});

// ─── The mirrored body ───────────────────────────────────────────────────────

describe("fetchLatestMirroredInbound", () => {
  it("reads the newest INBOUND message, falling back to html when text is empty", async () => {
    mockExecute.mockResolvedValue(
      pgResult([
        { instantly_email_id: "e-9", body_text: "", body_html: "<p>Please remove me.</p>" },
      ]),
    );

    const inbound = await fetchLatestMirroredInbound("camp-1");
    expect(inbound?.instantlyEmailId).toBe("e-9");
    expect(inbound?.text).toContain("Please remove me.");

    const sqlText = JSON.stringify(mockExecute.mock.calls[0][0]);
    // Inbound only (type 1 is outbound), newest first.
    expect(sqlText).toContain("ue_type");
    expect(sqlText).toContain("DESC");
  });

  it("returns null when the mirror holds nothing inbound", async () => {
    mockExecute.mockResolvedValue(pgResult([]));
    expect(await fetchLatestMirroredInbound("camp-1")).toBeNull();
  });

  it("returns null on an empty body rather than judging an empty string", async () => {
    mockExecute.mockResolvedValue(
      pgResult([{ instantly_email_id: "e-1", body_text: "   ", body_html: "" }]),
    );
    expect(await fetchLatestMirroredInbound("camp-1")).toBeNull();
  });
});

// ─── The promoteEvent side effect ────────────────────────────────────────────

describe("maybeRecordOptOutFromReply", () => {
  function mirrorHolds(text: string) {
    mockExecute.mockResolvedValue(
      pgResult([{ instantly_email_id: "e-1", body_text: text, body_html: "" }]),
    );
  }

  it("judges the reply and records the opt-out", async () => {
    mirrorHolds("Please take me off of your email list.");
    mockQualifyReply.mockResolvedValue(OPT_OUT_REPLY_KIND);

    await maybeRecordOptOutFromReply(CAMPAIGN, "reply_received");

    expect(mockRecordLeadOptOut).toHaveBeenCalledTimes(1);
  });

  it("fires on a reply-KIND event too, not only on reply_received", async () => {
    // Instantly's own qualification lands as a separate event moments after the
    // reply webhook, and the mirror taken at the earlier moment may not hold the
    // message yet. Several attempts; the record itself is idempotent.
    mirrorHolds("unsubscribe");
    mockQualifyReply.mockResolvedValue(OPT_OUT_REPLY_KIND);

    await maybeRecordOptOutFromReply(CAMPAIGN, "lead_not_interested");

    expect(mockRecordLeadOptOut).toHaveBeenCalledTimes(1);
  });

  it("no-ops on an event that carries no inbound mail", async () => {
    await maybeRecordOptOutFromReply(CAMPAIGN, "email_sent");
    expect(mockExecute).not.toHaveBeenCalled();
    expect(mockQualifyReply).not.toHaveBeenCalled();
  });

  it("no-ops on a self-send sequence — the IMAP poller owns it", async () => {
    await maybeRecordOptOutFromReply(
      { ...CAMPAIGN, instantlyCampaignId: "self:abc" },
      "reply_received",
    );
    expect(mockQualifyReply).not.toHaveBeenCalled();
  });

  it("no-ops on a reservation sentinel", async () => {
    await maybeRecordOptOutFromReply(
      { ...CAMPAIGN, instantlyCampaignId: "reserving:abc" },
      "reply_received",
    );
    expect(mockQualifyReply).not.toHaveBeenCalled();
  });

  it("pays for NO model call when the person already stands opted out", async () => {
    mockFindStandingOptOut.mockResolvedValue({ id: "o-1" });

    await maybeRecordOptOutFromReply(CAMPAIGN, "reply_received");

    expect(mockQualifyReply).not.toHaveBeenCalled();
    expect(mockRecordLeadOptOut).not.toHaveBeenCalled();
  });

  it("SWALLOWS a classifier failure — it runs inside Instantly's webhook", async () => {
    // A throw here becomes a 5xx, which Instantly counts toward disabling the
    // whole subscription. That has already cost this service a six-day outage.
    mirrorHolds("unsubscribe");
    mockQualifyReply.mockRejectedValue(new Error("chat-service down"));

    await expect(maybeRecordOptOutFromReply(CAMPAIGN, "reply_received")).resolves.toBeUndefined();
  });
});

// ─── The self-send call site ─────────────────────────────────────────────────
//
// `runPoll` needs a live IMAP session, so it has no unit-level driver. The wire
// is pinned at the source instead — the component being correct is not the same
// fact as the caller reaching it, and this repo has shipped that exact gap
// before (a prop threaded into a page that never passed it).

describe("the IMAP poller's call site", () => {
  const src = readFileSync("src/lib/self-send/imap-poller.ts", "utf-8");

  it("records the opt-out on the classifier's own verdict", () => {
    const block = src.slice(src.indexOf("const qualification = await qualifyReply"));
    expect(block).toContain("qualification === OPT_OUT_REPLY_KIND");
    expect(block).toContain("recordOptOutFromReply");
  });

  it("passes the classification through — no second model call for one answer", () => {
    const call = src.slice(
      src.indexOf("await recordOptOutFromReply({"),
      src.indexOf("await recordOptOutFromReply({") + 800,
    );
    expect(call).toContain("qualification,");
    expect(call).toContain("replyText,");
  });
});

// ─── The line between a decline and a request to stop ────────────────────────
//
// The classifier is a model, so its verdict cannot be asserted in a unit test.
// What CAN be pinned is the rule it is given — and these six examples are the
// rule, taken from replies this fleet actually received. A prompt edit that
// drops them fails here rather than silently re-filing opt-outs as declines.

describe("declining vs asking to stop", () => {
  const OPT_OUT_EXAMPLES = [
    "Stop",
    "Unsubscribe",
    // A misspelling is still the request, and it is the single strongest
    // argument against a keyword pre-filter on this path.
    "unsusbsribe",
    "No interest, please stop sending emails.",
  ];

  const DECLINE_EXAMPLES = ["Not for us, thanks.", "No interest"];

  it("gives the model every opt-out example verbatim", () => {
    for (const example of OPT_OUT_EXAMPLES) {
      expect(SYSTEM_PROMPT).toContain(`"${example}" -> lead_opt_out_requested`);
    }
  });

  it("gives it the declines too — 'no interest' alone is NOT a request to stop", () => {
    for (const example of DECLINE_EXAMPLES) {
      expect(SYSTEM_PROMPT).toContain(`"${example}" -> lead_not_interested`);
    }
  });

  it("states that a decline carrying a removal request is an opt-out", () => {
    // "No interest, please stop sending emails." is an opt-out on its SECOND
    // clause. Reading only the first is how all seven prod cases were misfiled.
    expect(SYSTEM_PROMPT).toContain("even if it also declines the offer");
  });

  it("tells it to ignore OUR unsubscribe footer quoted back at it", () => {
    expect(SYSTEM_PROMPT).toContain("That is OUR footer");
  });
});
