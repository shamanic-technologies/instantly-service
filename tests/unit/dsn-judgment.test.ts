import { describe, it, expect, vi, beforeEach } from "vitest";

import { GMAIL_ADDRESS_NOT_FOUND_DSN, GODADDY_SPAM_BLOCK_DSN } from "../helpers/dsn-fixtures";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockDbExecute(...a) } }));
const promoteEventMock = vi.fn(async () => ({ promoted: true, silverEventId: "e-new" }));
vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...a: unknown[]) => promoteEventMock(...(a as [])),
}));
const judgmentMock = vi.fn();
vi.mock("../../src/lib/chat-client", () => ({
  platformJudgment: (...a: unknown[]) => judgmentMock(...(a as [])),
}));
const retractMock = vi.fn(async () => {});
vi.mock("../../src/lib/bounce-retraction", () => ({
  retractBouncedEvents: (...a: unknown[]) => retractMock(...(a as [])),
}));

import {
  buildDsnQuestion,
  buildDsnState,
  DSN_QUESTION_KEY,
  planDsnSettlement,
  readDsnJudgment,
  type BounceEventRef,
} from "../../src/lib/self-send/dsn-judgment";
import { runDsnJudgmentSweep } from "../../src/lib/self-send/dsn-judgment-sweep";

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

const GODADDY_ROW = {
  id: "9c5d0bdb-2dc8-4b37-8c1c-1c3a4dec789f",
  account_email: "raquel@agileconsultco.com",
  instantly_campaign_id: "self:e20a05d6-bd5b-4b78-9dbc-288007bff6eb",
  step: 1,
  lead_email: "team@definitejobsgroup.co.uk",
  from_address: '"Mail Delivery Subsystem" <mailer-daemon@googlemail.com>',
  subject: "Delivery Status Notification (Failure)",
  text: GODADDY_SPAM_BLOCK_DSN,
  at: "2026-10-15T19:12:26Z",
  judgment: null,
};

const GMAIL_ROW = {
  id: "row-gmail-dead",
  account_email: "raquel@agileconsultco.com",
  instantly_campaign_id: "self:dead-address",
  step: 1,
  lead_email: "jane.doe@example-clinic.com",
  from_address: '"Mail Delivery Subsystem" <mailer-daemon@googlemail.com>',
  subject: "Delivery Status Notification (Failure)",
  text: GMAIL_ADDRESS_NOT_FOUND_DSN,
  at: "2026-10-16T09:00:00Z",
  judgment: null,
};

/** The bounce the old poll promoted from the GoDaddy notice (seq 7935225's event). */
const GODADDY_EVENT = {
  id: "124aaee6-d171-40f3-a571-1fe8c89beea3",
  campaign_id: GODADDY_ROW.instantly_campaign_id,
  lead_email: GODADDY_ROW.lead_email,
  step: 1,
  source_row_id: GODADDY_ROW.id,
  source: "self_send",
};

function database(rows: Array<Record<string, unknown>>, events: Array<Record<string, unknown>>) {
  mockDbExecute.mockImplementation(async (query: unknown) => {
    const text = JSON.stringify(query);
    if (text.includes("UPDATE imap_messages_raw")) return pgResult([]);
    if (text.includes("FROM imap_messages_raw")) return pgResult(rows);
    if (text.includes("FROM instantly_events")) return pgResult(events);
    return pgResult([]);
  });
}

/** A judgment answering `choice` for whichever DSN the state carries. */
function judges(verdictFor: (state: string) => string) {
  judgmentMock.mockImplementation(async ({ state }: { state: string }) => {
    const choice = verdictFor(state);
    const other = choice === "address_dead" ? "message_refused" : "address_dead";
    return {
      model: "jev-test",
      answers: {
        [DSN_QUESTION_KEY]: {
          type: "choice",
          choice,
          confidence: 0.9,
          probabilities: { [choice]: 0.95, [other]: 0.05 },
        },
      },
      usage: { inputTokens: 300, outputTokens: 0 },
    };
  });
}

const byFixture = (state: string) =>
  state.includes("content judged to be spam") ? "message_refused" : "address_dead";

beforeEach(() => {
  mockDbExecute.mockReset();
  promoteEventMock.mockClear();
  judgmentMock.mockReset();
  retractMock.mockClear();
});

describe("a refused MESSAGE is not a dead address (BOUNCE-SPAM-BLOCK-1018)", () => {
  it("GoDaddy 'Message blocked ... content judged to be spam' is not a bounce: no promotion, the old bounce retracted", async () => {
    database([GODADDY_ROW], [GODADDY_EVENT]);
    judges(byFixture);

    const summary = await runDsnJudgmentSweep({ dryRun: false, sinceDays: null });

    // The judgment read the server's own words, not a code.
    const state = (judgmentMock.mock.calls[0]![0] as { state: string }).state;
    expect(state).toContain("552 5.2.0");
    expect(state).toContain("content judged to be spam");

    expect(promoteEventMock).not.toHaveBeenCalled();
    expect(retractMock).toHaveBeenCalledTimes(1);
    const [events, reason] = retractMock.mock.calls[0] as unknown as [BounceEventRef[], string];
    expect(events.map((e) => e.id)).toEqual([GODADDY_EVENT.id]);
    expect(reason).toBe("message_refused_not_a_bounce");
    expect(summary).toMatchObject({ messageRefused: 1, addressDead: 0, bounceEventsRetracted: 1, peopleCleared: 1 });
    expect(summary.retractedLeads).toEqual(["team@definitejobsgroup.co.uk"]);
  });

  it("Gmail '550 5.1.1 address not found' IS a bounce: email_bounced promoted on its send", async () => {
    database([GMAIL_ROW], []);
    judges(byFixture);

    const summary = await runDsnJudgmentSweep({ dryRun: false });

    expect(promoteEventMock).toHaveBeenCalledTimes(1);
    expect(promoteEventMock.mock.calls[0]![0]).toMatchObject({
      eventType: "email_bounced",
      instantlyCampaignId: "self:dead-address",
      leadEmail: "jane.doe@example-clinic.com",
      step: 1,
      source: "self_send",
      sourceRowId: "row-gmail-dead",
    });
    expect(retractMock).toHaveBeenCalledWith([], "message_refused_not_a_bounce", expect.any(String));
    expect(summary).toMatchObject({ addressDead: 1, messageRefused: 0, bouncesPromoted: 1 });
  });

  it("persists the judgment on the row, and a judged row is never asked again", async () => {
    database([GODADDY_ROW], [GODADDY_EVENT]);
    judges(byFixture);
    await runDsnJudgmentSweep({ dryRun: true, sinceDays: null });
    const update = mockDbExecute.mock.calls
      .map((c) => JSON.stringify(c[0]))
      .find((t) => t.includes("UPDATE imap_messages_raw"));
    expect(update).toContain("dsnJudgment");
    expect(update).toContain("message_refused");

    judgmentMock.mockClear();
    const judged = {
      ...GODADDY_ROW,
      judgment: { verdict: "message_refused", questionVersion: 3, judgedAt: "x", model: "m", probability: 1, confidence: 1, probabilities: {} },
    };
    database([judged], [GODADDY_EVENT]);
    await runDsnJudgmentSweep({ dryRun: false, sinceDays: null });
    expect(judgmentMock).not.toHaveBeenCalled();
    expect(retractMock).toHaveBeenCalledTimes(1);
  });

  it("a judgment asked under an older wording of the question is asked again", async () => {
    const stale = {
      ...GODADDY_ROW,
      judgment: { verdict: "address_dead", questionVersion: 1, judgedAt: "x", model: "m", probability: 1, confidence: 1, probabilities: {} },
    };
    database([stale], [GODADDY_EVENT]);
    judges(byFixture);
    const summary = await runDsnJudgmentSweep({ dryRun: true, sinceDays: null });
    expect(judgmentMock).toHaveBeenCalledTimes(1);
    expect(summary).toMatchObject({ messageRefused: 1, bounceEventsRetracted: 1 });
  });

  it("a dry run judges but retracts and promotes nothing", async () => {
    database([GODADDY_ROW, GMAIL_ROW], [GODADDY_EVENT]);
    judges(byFixture);
    const summary = await runDsnJudgmentSweep({ dryRun: true, sinceDays: null });
    expect(retractMock).not.toHaveBeenCalled();
    expect(promoteEventMock).not.toHaveBeenCalled();
    expect(summary).toMatchObject({ bounceEventsRetracted: 1, bouncesPromoted: 1, retractedBySource: { self_send: 1 } });
  });

  it("a failed judgment leaves the DSN unjudged: no bounce, no retraction, retried next run", async () => {
    database([GMAIL_ROW], []);
    judgmentMock.mockRejectedValue(new Error("chat-service 502"));
    const summary = await runDsnJudgmentSweep({ dryRun: false });
    expect(promoteEventMock).not.toHaveBeenCalled();
    expect(summary).toMatchObject({ judgmentFailed: 1, unjudged: 1, bouncesPromoted: 0 });
  });
});

describe("the judgment itself", () => {
  it("offers exactly two options and puts the burden of proof on 'dead'", () => {
    const q = buildDsnQuestion();
    expect(Object.keys(q.criteria).sort()).toEqual(["address_dead", "message_refused"]);
    expect(q.instructions).toMatch(/not on the numeric status code/);
    expect(q.instructions).toMatch(/do not say the address is gone, answer message_refused/);
  });

  it("an option outside the two is a bug, never a default", () => {
    expect(() =>
      readDsnJudgment({ type: "choice", choice: "maybe", confidence: 1, probabilities: { maybe: 1 } }, "m", new Date()),
    ).toThrow(/unknown option/);
  });

  it("the state carries the DSN's words", () => {
    const state = buildDsnState({ from: "mailer-daemon@googlemail.com", subject: "x", text: GODADDY_SPAM_BLOCK_DSN });
    expect(state).toContain("Message blocked");
  });
});

describe("planDsnSettlement (grain = one send)", () => {
  const ev = (over: Partial<BounceEventRef>): BounceEventRef => ({
    id: "e",
    campaignId: "c1",
    leadEmail: "p@x.com",
    step: 1,
    sourceRowId: null,
    source: "self_send",
    ...over,
  });

  it("a refused send loses EVERY bounce on it, the webhook's included", () => {
    const plan = planDsnSettlement(
      [{ id: "r1", instantlyCampaignId: "c1", step: 1, verdict: "message_refused" }],
      new Set(),
      [ev({ id: "e1", sourceRowId: "r1" }), ev({ id: "e2", source: "webhook" }), ev({ id: "e3", step: 2 })],
    );
    expect(plan.retract.map((e) => e.id)).toEqual(["e1", "e2"]);
    expect(plan.promote).toEqual([]);
  });

  it("one DSN saying the address is dead keeps the send a bounce", () => {
    const plan = planDsnSettlement(
      [
        { id: "r1", instantlyCampaignId: "c1", step: 1, verdict: "message_refused" },
        { id: "r2", instantlyCampaignId: "c1", step: 1, verdict: "address_dead" },
      ],
      new Set(),
      [ev({ id: "e1", sourceRowId: "r1" })],
    );
    expect(plan.retract).toEqual([]);
    expect(plan.promote).toEqual([]);
  });

  it("a send with an unjudged DSN is left alone", () => {
    const plan = planDsnSettlement(
      [{ id: "r1", instantlyCampaignId: "c1", step: 1, verdict: "message_refused" }],
      new Set(["c1|1"]),
      [ev({ id: "e1" })],
    );
    expect(plan.retract).toEqual([]);
  });

  it("a dead address with no bounce yet is promoted once per send", () => {
    const plan = planDsnSettlement(
      [
        { id: "r1", instantlyCampaignId: "c1", step: 1, verdict: "address_dead" },
        { id: "r2", instantlyCampaignId: "c1", step: 1, verdict: "address_dead" },
      ],
      new Set(),
      [],
    );
    expect(plan.promote.map((p) => p.id)).toEqual(["r1"]);
  });
});
