import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));

import { backfillReplySends, matchPastReplies, type PastReply, type SenderRun } from "../../src/lib/reply-sends-backfill";

/**
 * REPLY-BACKFILL-1119: a past answer is declared on the run that sent it only on a POSITIVE key (same org,
 * inside the run's window, the same thread or a new thread to the same address, one run for one answer);
 * time proximity alone never matches.
 */
const ORG = "8c043fa7-88b9-4e92-a96b-3dab875cb465";
const LEAD = "e5a7a3ba-8fc9-49c9-b0d4-cc9279732244";
const RUN = "a7af6746-7a45-4ae5-b92e-0219be4bfabd";
const THREAD = "<b796eb8f@hellodistribute.com>";

const reply = (over: Partial<PastReply> = {}): PastReply => ({
  dispatchId: "d-1",
  orgId: ORG,
  leadId: LEAD,
  instantlyCampaignId: "ic-1",
  leadEmail: "rorys@forthright.com",
  sentAt: "2026-11-16T13:13:44.000Z",
  sentBy: "automation",
  messageId: "<m1@x>",
  inReplyTo: THREAD,
  newThread: false,
  chosenTo: [],
  transport: "smtp",
  ...over,
});
const run = (over: Partial<SenderRun> = {}): SenderRun => ({
  scriptRunId: "sr-1",
  orgId: ORG,
  runId: RUN,
  createdAt: "2026-11-16T13:13:30.000Z",
  finishedAt: "2026-11-16T13:13:45.000Z",
  okSendCalls: 1,
  replyTo: THREAD,
  inThread: true,
  to: ["rorys@forthright.com"],
  leadIds: [],
  ...over,
});
const verdict = (replies: PastReply[], runs: SenderRun[], declared: string[] = []) => matchPastReplies(replies, runs, new Set(declared));

describe("matchPastReplies: positive key only", () => {
  it("same org, inside the window, same thread = the run that sent it", () => {
    expect(verdict([reply()], [run()]).get("d-1")).toEqual({ kind: "match", run: run(), key: "thread" });
  });

  it("a run in the window that answered ANOTHER thread is not a match (time alone never is)", () => {
    expect(verdict([reply()], [run({ replyTo: "<other@x>" })]).get("d-1")).toEqual({ kind: "unmatched", reason: "sender_run_without_thread_key" });
    expect(verdict([reply()], [run({ replyTo: null, to: [] })]).get("d-1")).toEqual({ kind: "unmatched", reason: "sender_run_without_thread_key" });
  });

  it("outside the window, another org, or a run with two sends: no sender run", () => {
    expect(verdict([reply({ sentAt: "2026-11-16T13:14:00.000Z" })], [run()]).get("d-1")).toMatchObject({ reason: "no_sender_run_on_record" });
    expect(verdict([reply({ orgId: "other" })], [run()]).get("d-1")).toMatchObject({ reason: "no_sender_run_on_record" });
    expect(verdict([reply()], [run({ okSendCalls: 2 })]).get("d-1")).toMatchObject({ reason: "no_sender_run_on_record" });
  });

  it("the run's named To must be who the reply went to (the chosen To, else the lead)", () => {
    expect(verdict([reply({ chosenTo: ["billing@x.com"] })], [run({ to: ["billing@x.com"] })]).get("d-1")).toMatchObject({ kind: "match" });
    expect(verdict([reply()], [run({ to: ["someone@else.com"] })]).get("d-1")).toMatchObject({ kind: "unmatched" });
    // No To named by the run = the thread's lead.
    expect(verdict([reply()], [run({ to: [] })]).get("d-1")).toMatchObject({ kind: "match" });
  });

  it("a new thread matches only a run that opened a new thread to the same address", () => {
    const nt = reply({ inReplyTo: null, newThread: true });
    expect(verdict([nt], [run({ replyTo: null, inThread: false })]).get("d-1")).toMatchObject({ kind: "match", key: "new_thread" });
    expect(verdict([nt], [run({ replyTo: null, inThread: false, to: [] })]).get("d-1")).toMatchObject({ kind: "unmatched" });
    expect(verdict([nt], [run({ replyTo: null, inThread: true })]).get("d-1")).toMatchObject({ kind: "unmatched" });
  });

  it("a lead the run declared it consumed for must be the reply's lead", () => {
    expect(verdict([reply()], [run({ leadIds: [LEAD] })]).get("d-1")).toMatchObject({ kind: "match" });
    expect(verdict([reply()], [run({ leadIds: ["another-lead"] })]).get("d-1")).toMatchObject({ kind: "unmatched" });
  });

  it("one run for one reply, both ways; never declared when ambiguous", () => {
    expect(verdict([reply()], [run(), run({ scriptRunId: "sr-2", runId: "r-2" })]).get("d-1")).toMatchObject({ reason: "ambiguous_runs" });
    const v = verdict([reply(), reply({ dispatchId: "d-2", messageId: "<m2@x>" })], [run()]);
    expect(v.get("d-1")).toMatchObject({ reason: "run_claimed_by_several_replies" });
    expect(v.get("d-2")).toMatchObject({ reason: "run_claimed_by_several_replies" });
  });

  it("before the ledger's start, a hand reply with no sender run, no lead id, a run already declared: out", () => {
    expect(verdict([reply({ sentAt: "2026-10-22T23:59:59.000Z" })], [run()]).get("d-1")).toMatchObject({ reason: "before_ledger_start" });
    expect(verdict([reply({ sentBy: "human" })], []).get("d-1")).toMatchObject({ reason: "hand_sent_no_sender_run" });
    expect(verdict([reply({ leadId: null })], [run()]).get("d-1")).toMatchObject({ reason: "no_lead_id" });
    expect(verdict([reply()], [run()], [RUN]).get("d-1")).toMatchObject({ reason: "run_already_declared_an_answer" });
  });
});

describe("backfillReplySends", () => {
  const pg = (rows: unknown[]) => ({ rows });
  const row = { id: "d-1", org_id: ORG, lead_id: LEAD, instantly_campaign_id: "ic-1", lead_email: "rorys@forthright.com", sent_at: "2026-11-16T13:13:44.000Z", sent_by: "automation", message_id: "<m1@x>", in_reply_to: THREAD, new_thread: null, chosen_to: null };
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());
  const text = (call: unknown[]) => JSON.stringify((call[0] as { queryChunks: unknown[] }).queryChunks);

  it("dry run by default: counts, calls nobody, writes nothing", async () => {
    execute.mockResolvedValueOnce(pg([row])).mockResolvedValueOnce(pg([]));
    const report = await backfillReplySends({ senderRuns: [run()] });
    expect(report).toMatchObject({ dryRun: true, read: 1, matched: 1, matchedBy: { thread: 1 }, perOrg: { [ORG]: { matched_thread: 1 } } });
    expect(fetchMock).not.toHaveBeenCalled();
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("apply declares through the live declaration (message_sent on the run, dated at the send, nothing consumed) and keeps the row marked", async () => {
    execute.mockResolvedValueOnce(pg([row])).mockResolvedValueOnce(pg([])).mockResolvedValue(pg([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":1,"alreadyRecordedCount":0}', { status: 201 }));
    const report = await backfillReplySends({ senderRuns: [run()], apply: true, limit: 1 });
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      runs: [{ runId: RUN, produced: [{ outcomeKey: "message_sent", subjectService: "lead-service", subjectId: LEAD }], consumed: [], occurredAt: row.sent_at }],
    });
    expect(report.applied).toEqual({ recorded: 1, alreadyRecorded: 0, runGone: 0, refused: 0, retry: 0 });
    expect(text(execute.mock.calls[2])).toContain("INSERT INTO reply_send_outcomes");
    expect((execute.mock.calls[2][0] as { queryChunks: unknown[] }).queryChunks).toContain("declared");
  });

  it("an unreadable ledger answer keeps nothing (a re-run retries it)", async () => {
    const loud = vi.spyOn(console, "error").mockImplementation(() => {});
    execute.mockResolvedValueOnce(pg([row])).mockResolvedValueOnce(pg([]));
    fetchMock.mockResolvedValue(new Response("boom", { status: 503 }));
    const report = await backfillReplySends({ senderRuns: [run()], apply: true, limit: 1 });
    expect(report.applied.retry).toBe(1);
    expect(execute).toHaveBeenCalledTimes(2);
    loud.mockRestore();
  });
});
