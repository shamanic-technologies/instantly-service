import { describe, expect, it } from "vitest";
import {
  drainLimitFor,
  mailboxesWithOverdueFollowUps,
  type PendingSequence,
} from "../../src/lib/self-send/dispatch";
import { isLoginFailure, silencedSenderDetails } from "../../src/lib/self-send/sender-health";

// SENDING-INFRA-AUDIT-1106: the prod symptoms of 2026-11-06, replayed.

const ASOF = new Date("2026-11-06T15:00:00.000Z");

function seq(over: Partial<PendingSequence>): PendingSequence {
  return {
    instantlyCampaignId: "self:1",
    leadEmail: "lead@x.com",
    accountEmail: "kevin.l@leaddistribute.com",
    provisionedSteps: [2, 3],
    lastSentStep: 1,
    lastSentAt: new Date("2026-10-29T14:00:00.000Z"),
    stepDelays: [3, 7, null],
    ...over,
  };
}

describe("a recovering mailbox owing late follow-ups drains them at the production limit", () => {
  const mailboxOf = (a: string) => a;

  it("finds the mailbox carrying a follow-up due before today", () => {
    expect([...mailboxesWithOverdueFollowUps([seq({})], mailboxOf, ASOF)]).toEqual(["kevin.l@leaddistribute.com"]);
  });

  it("does not count a follow-up that came due TODAY, nor a first email", () => {
    const today = seq({ lastSentAt: new Date("2026-11-03T09:00:00.000Z") }); // +3 d = today
    const first = seq({ instantlyCampaignId: "self:2", lastSentStep: null, lastSentAt: null, provisionedSteps: [1, 2], queuedAt: new Date("2026-10-20T00:00:00.000Z") });
    expect(mailboxesWithOverdueFollowUps([today, first], mailboxOf, ASOF).size).toBe(0);
  });

  it("raises ONLY an in_recovery mailbox with overdue follow-ups, never a stopped one", () => {
    expect(drainLimitFor(20, "in_recovery", true, 50)).toBe(50);
    expect(drainLimitFor(20, "in_recovery", false, 50)).toBe(20);
    expect(drainLimitFor(20, "deactivated_by_user", true, 50)).toBe(20);
    expect(drainLimitFor(0, "in_recovery", true, 50)).toBe(0);
    expect(drainLimitFor(50, "in_production", true, 50)).toBe(50);
  });
});

describe("a login failure is named as one (kevin@pressbeat.ai, 2026-11-05)", () => {
  it("recognises the 535 forms Google and Gandi answer", () => {
    expect(isLoginFailure("535-5.7.8 Username and Password not accepted.", 535)).toBe(true);
    expect(isLoginFailure("Invalid login: 535 5.7.8 Error: authentication failed", null)).toBe(true);
    expect(isLoginFailure("550-5.4.5 Daily user sending limit exceeded", 550)).toBe(false);
    expect(isLoginFailure("550 5.7.1 Blacklisted user", 550)).toBe(false);
  });

  it("serves the reason and the start of the refusals for a silenced mailbox only", () => {
    const details = silencedSenderDetails([
      {
        mailbox: "kevin@pressbeat.ai",
        lastSuccessAt: new Date("2026-11-04T10:00:00.000Z"),
        failures: [{ response: "535 5.7.8 Error: authentication failed", responseCode: 535, count: 13, firstAt: new Date("2026-11-05T08:00:00.000Z") }],
      },
      {
        mailbox: "ok@x.com",
        lastSuccessAt: new Date("2026-11-06T10:00:00.000Z"),
        failures: [{ response: "535 5.7.8 Error: authentication failed", responseCode: 535, count: 3, firstAt: new Date("2026-11-05T08:00:00.000Z") }],
      },
    ]);
    expect([...details.keys()]).toEqual(["kevin@pressbeat.ai"]);
    expect(details.get("kevin@pressbeat.ai")).toMatchObject({ loginFailed: true, since: new Date("2026-11-05T08:00:00.000Z") });
  });
});

describe("warmup keeps to its share on a mailbox that owes prospect mail (kevin.l@smartdistribute.com, 2026-11-06)", () => {
  it("fills the headroom only when nothing is owed", async () => {
    const { owesNoProspectMail, warmupBudgetFor } = await import("../../src/lib/warmup/plan");
    expect(owesNoProspectMail(22)).toBe(false);
    expect(owesNoProspectMail(0)).toBe(true);
    // cap 20 in recovery: headroom = 20 (the whole cap), share = 4 (<= 30%).
    expect(warmupBudgetFor(20, { fillsHeadroom: owesNoProspectMail(22) })).toBeLessThanOrEqual(Math.floor(20 * 0.3));
    expect(warmupBudgetFor(20, { fillsHeadroom: owesNoProspectMail(0) })).toBe(20);
  });
});

describe("follow-ups already owed take the room before a new thread opens (2026-11-04/05: 2,082 first emails sent while follow-ups waited)", () => {
  it("leaves out the first emails that would spend a follow-up's room, on its mailbox and across the fleet", async () => {
    const { holdFirstEmailsBehindFollowUps, selectDueSteps } = await import("../../src/lib/self-send/dispatch");
    // Friday 15:00 UTC = 10:00 Chicago, inside the window.
    const asOf = new Date("2026-11-06T15:00:00.000Z");
    const cap = (accountEmail: string, room: number, adopts: boolean) => ({
      accountEmail, mailbox: accountEmail, cap: 50, recentSustainedDaily: 50, sentToday: 50 - room, adoptsFirstEmails: adopts,
    });
    const followUp = (i: number) => seq({ instantlyCampaignId: `self:f${i}`, leadEmail: `f${i}@x.com`, accountEmail: "busy@x.com", timezone: "America/Chicago" });
    // A first email queued BEFORE the follow-ups came due: it sorts first.
    const first = (i: number, account: string) => seq({
      instantlyCampaignId: `self:n${i}`, leadEmail: `n${i}@x.com`, accountEmail: account, lastSentStep: null, lastSentAt: null,
      provisionedSteps: [1, 2, 3], queuedAt: new Date("2026-10-20T00:00:00.000Z"), timezone: "America/Chicago",
    });
    const sequences = [followUp(1), followUp(2), followUp(3), first(1, "busy@x.com"), first(2, "busy@x.com"), first(3, "busy@x.com")];
    const capacities = [cap("busy@x.com", 3, true), cap("spare@x.com", 1, true)];

    // Before: the old first emails take busy's 3 slots and the follow-ups wait.
    const before = selectDueSteps(sequences, capacities, asOf);
    expect(before.selected.filter((s) => s.instantlyCampaignId.startsWith("self:f"))).toHaveLength(0);

    const { kept, held, moves } = holdFirstEmailsBehindFollowUps(sequences, capacities, asOf);
    // busy has room for its 3 follow-ups only; spare can adopt ONE first email.
    expect(held).toBe(2);
    expect(moves).toEqual([{ instantlyCampaignId: "self:n1", from: "busy@x.com", to: "spare@x.com" }]);
    const after = selectDueSteps(kept, capacities, asOf);
    expect(after.selected.filter((s) => s.instantlyCampaignId.startsWith("self:f"))).toHaveLength(3);
    expect(after.selected.filter((s) => s.accountEmail === "spare@x.com")).toHaveLength(1);
  });

  it("never holds an answer owed to a person", async () => {
    const { holdFirstEmailsBehindFollowUps } = await import("../../src/lib/self-send/dispatch");
    const asOf = new Date("2026-11-06T15:00:00.000Z");
    const answer = seq({ lastSentStep: null, lastSentAt: null, provisionedSteps: [1], answersPerson: true, timezone: "America/Chicago" });
    const { held } = holdFirstEmailsBehindFollowUps([answer], [{ accountEmail: "kevin.l@leaddistribute.com", mailbox: "kevin.l@leaddistribute.com", cap: 20, recentSustainedDaily: 20, sentToday: 20 }], asOf);
    expect(held).toBe(0);
  });
});

describe("a draining mailbox's raised room goes to its follow-ups, its first emails move to production", () => {
  it("moves the first email of a mailbox draining at a raised limit even when that mailbox has room", async () => {
    const { holdFirstEmailsBehindFollowUps } = await import("../../src/lib/self-send/dispatch");
    const asOf = new Date("2026-11-06T15:00:00.000Z");
    const first = seq({ lastSentStep: null, lastSentAt: null, provisionedSteps: [1, 2], queuedAt: new Date("2026-11-05T00:00:00.000Z"), timezone: "America/Chicago" });
    const res = holdFirstEmailsBehindFollowUps(
      [first],
      [
        { accountEmail: "kevin.l@leaddistribute.com", mailbox: "kevin.l@leaddistribute.com", cap: 50, recentSustainedDaily: 50, sentToday: 0, adoptsFirstEmails: false },
        { accountEmail: "prod@x.com", mailbox: "prod@x.com", cap: 50, recentSustainedDaily: 50, sentToday: 49, adoptsFirstEmails: true },
      ],
      asOf,
      new Set(),
      new Set(["kevin.l@leaddistribute.com"]),
    );
    expect(res.moves).toEqual([{ instantlyCampaignId: "self:1", from: "kevin.l@leaddistribute.com", to: "prod@x.com" }]);
    expect(res.held).toBe(0);
  });
});
