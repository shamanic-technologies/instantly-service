import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));

const mockQualifyReply = vi.fn();
vi.mock("../../src/lib/self-send/qualify-reply", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  qualifyReply: (...a: unknown[]) => mockQualifyReply(...a),
}));

const mockFetchLatestMirroredInbound = vi.fn();
const mockRecordOptOutFromReply = vi.fn();
vi.mock("../../src/lib/reply-opt-out", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  fetchLatestMirroredInbound: (...a: unknown[]) => mockFetchLatestMirroredInbound(...a),
  recordOptOutFromReply: (...a: unknown[]) => mockRecordOptOutFromReply(...a),
}));

import { backfillReplyOptOuts } from "../../src/lib/reply-optout-backfill";

const CANDIDATE = {
  instantly_campaign_id: "camp-1",
  lead_email: "chad@clinic.com",
  org_id: "org-1",
};

beforeEach(() => {
  vi.resetAllMocks();
  mockExecute.mockResolvedValue(pgResult([CANDIDATE]));
  mockFetchLatestMirroredInbound.mockResolvedValue({
    instantlyEmailId: "e-1",
    text: "Please remove me from your email list.",
  });
});

describe("candidate selection", () => {
  it("excludes self-send, reservation sentinels and anyone already opted out", async () => {
    await backfillReplyOptOuts({ dryRun: true });
    const sqlText = JSON.stringify(mockExecute.mock.calls[0][0]);

    expect(sqlText).toContain("self:%");
    expect(sqlText).toContain("reserving:%");
    // A standing (non-withdrawn) consent record takes the lead out of the set —
    // which is also what makes a second run report zero.
    expect(sqlText).toContain("instantly_lead_optouts_raw");
    expect(sqlText).toContain("instantly_lead_optout_withdrawals");
    // Inbound only.
    expect(sqlText).toContain("ue_type");
  });

  it("orders live sequences first — those are the ones still emailing", async () => {
    await backfillReplyOptOuts({ dryRun: true });
    const sqlText = JSON.stringify(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("active");
  });

  it("applies a caller limit and none otherwise", async () => {
    await backfillReplyOptOuts({ dryRun: true, limit: 5 });
    expect(JSON.stringify(mockExecute.mock.calls[0][0])).toContain("LIMIT");

    mockExecute.mockClear();
    await backfillReplyOptOuts({ dryRun: true });
    expect(JSON.stringify(mockExecute.mock.calls[0][0])).not.toContain("LIMIT");
  });
});

describe("dry run", () => {
  it("CLASSIFIES and names the leads, but records nothing", async () => {
    mockQualifyReply.mockResolvedValue("lead_opt_out_requested");

    const summary = await backfillReplyOptOuts({ dryRun: true });

    expect(summary.candidates).toBe(1);
    expect(summary.optOuts).toBe(1);
    expect(summary.recorded).toBe(0);
    expect(summary.leads).toEqual(["chad@clinic.com"]);
    expect(mockRecordOptOutFromReply).not.toHaveBeenCalled();
  });

  it("counts a reply that is not an opt-out without naming it", async () => {
    mockQualifyReply.mockResolvedValue("lead_not_interested");

    const summary = await backfillReplyOptOuts({ dryRun: true });

    expect(summary.judged).toBe(1);
    expect(summary.optOuts).toBe(0);
    expect(summary.leads).toEqual([]);
  });

  it("counts an unusable classification rather than reading it as 'no'", async () => {
    mockQualifyReply.mockResolvedValue(null);
    const summary = await backfillReplyOptOuts({ dryRun: true });
    expect(summary.unqualified).toBe(1);
    expect(summary.optOuts).toBe(0);
  });

  it("is the DEFAULT — a caller that says nothing writes nothing", async () => {
    mockQualifyReply.mockResolvedValue("lead_opt_out_requested");
    await backfillReplyOptOuts();
    expect(mockRecordOptOutFromReply).not.toHaveBeenCalled();
  });
});

describe("commit run", () => {
  it("records through the SAME helper the live path uses", async () => {
    mockRecordOptOutFromReply.mockResolvedValue({ recorded: true, campaignsAffected: 2 });

    const summary = await backfillReplyOptOuts({ dryRun: false });

    expect(mockRecordOptOutFromReply).toHaveBeenCalledTimes(1);
    expect(summary.recorded).toBe(1);
    expect(summary.leads).toEqual(["chad@clinic.com"]);
  });

  it("counts an already-standing record instead of re-recording it", async () => {
    mockRecordOptOutFromReply.mockResolvedValue({
      recorded: false,
      reason: "already_standing",
    });

    const summary = await backfillReplyOptOuts({ dryRun: false });
    expect(summary.alreadyStanding).toBe(1);
    expect(summary.recorded).toBe(0);
  });

  it("keeps sweeping when one candidate throws — a dead reply must not blind the rest", async () => {
    mockExecute.mockResolvedValue(
      pgResult([CANDIDATE, { ...CANDIDATE, instantly_campaign_id: "camp-2" }]),
    );
    mockRecordOptOutFromReply
      .mockRejectedValueOnce(new Error("chat-service down"))
      .mockResolvedValueOnce({ recorded: true, campaignsAffected: 1 });

    const summary = await backfillReplyOptOuts({ dryRun: false });

    expect(summary.failed).toBe(1);
    expect(summary.recorded).toBe(1);
  });

  it("skips a candidate whose mirror holds no readable body", async () => {
    mockFetchLatestMirroredInbound.mockResolvedValue(null);

    const summary = await backfillReplyOptOuts({ dryRun: false });

    expect(summary.judged).toBe(0);
    expect(mockRecordOptOutFromReply).not.toHaveBeenCalled();
  });
});
