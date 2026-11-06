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
