import { describe, expect, it } from "vitest";
import { compareByPickerOrder, domainState, projectCapFrom } from "../../src/lib/ops/pure";
import { capacityVerdict } from "../../src/lib/ops/capacity-verdict";
import { endOfMonthUtc, toExclusion } from "../../src/lib/ops/exclusions";
import { deriveMailboxes } from "../../src/lib/mailboxes-sync";
import { drainLimitFor, mailboxRoom } from "../../src/lib/self-send/dispatch";
import { PLACEMENT_ALERT_INBOX_PCT_BAR } from "../../src/lib/placement-alert";
import { PRODUCTION_DELIVERY_PCT_BAR } from "../../src/lib/account-lifecycle";

// SENDING-INFRA-AUDIT-1106: the ops reads say what the sender does.

const ASOF = new Date("2026-11-06T17:30:00.000Z");

describe("item 4: the mailbox cap is the dispatcher's", () => {
  it("kevin@growthagency.group: one production alias at 50, four recovering at 20, 27/day sustained = 41, not 20", () => {
    const aliases = [
      ["kevin@growthagency.group", "in_production", 50],
      ["kevinl@growthagency.group", "in_recovery", 20],
      ["kevin.lourd@growthagency.group", "in_recovery", 20],
      ["klourd@growthagency.group", "in_recovery", 20],
      ["lourd@growthagency.group", "in_recovery", 20],
    ] as const;
    const carrier = aliases.map(([e]) => e).sort()[0];
    const room = mailboxRoom(
      aliases.map(([email, status, limit]) => ({
        accountEmail: email,
        mailbox: "kevin@growthagency.group",
        cap: drainLimitFor(limit, status, false, 50),
        recentSustainedDaily: email === carrier ? 27 : 0,
        sentToday: 0,
        adoptsFirstEmails: status === "in_production",
      })),
    ).get("kevin@growthagency.group")!;
    expect(room.cap).toBe(41);
    // The bug the dispatcher had: the peak copied on all five rows read 135.
    const copied = mailboxRoom(
      aliases.map(([email, status, limit]) => ({
        accountEmail: email, mailbox: "m", cap: limit, recentSustainedDaily: 27, sentToday: 0, adoptsFirstEmails: status === "in_production",
      })),
    ).get("m")!;
    expect(copied.cap).toBe(50);
  });
});

describe("item 5: the ramp starts at today's cap", () => {
  it("day 0 is the cap, then the ramp toward the limit", () => {
    expect(projectCapFrom(20, 20, ASOF)).toEqual([{ date: "2026-11-06", cap: 20 }]);
    expect(projectCapFrom(29, 50, ASOF).map((d) => d.cap)).toEqual([29, 44, 50]);
    expect(projectCapFrom(50, 50, ASOF)).toHaveLength(1);
    expect(projectCapFrom(0, 50, ASOF)).toEqual([{ date: "2026-11-06", cap: 0 }]);
  });
});

describe("item 10: lists come in picker order", () => {
  it("ranked rows first by rank, then production, recovery, deactivated, gone last", () => {
    const rows = [
      { fillRank: null, lifecycle: "in_recovery", gone: false, name: "a" },
      { fillRank: 2, lifecycle: "in_production", gone: false, name: "z" },
      { fillRank: null, lifecycle: "in_production", gone: true, name: "b" },
      { fillRank: 1, lifecycle: "in_production", gone: false, name: "y" },
      { fillRank: null, lifecycle: "deactivated_by_user", gone: false, name: "c" },
      { fillRank: null, lifecycle: "in_production", gone: false, name: "d" },
    ];
    expect([...rows].sort(compareByPickerOrder).map((r) => r.name)).toEqual(["y", "z", "d", "a", "c", "b"]);
  });
});

describe("item 12: a domain's state, not its registrar lock", () => {
  const base = { absentSince: null, cancelledAt: null, deletionScheduled: false, expiresAt: new Date("2027-06-29T00:00:00Z"), autorenew: true };
  it("reads active / expiring / gone", () => {
    expect(domainState(base, ASOF)).toBe("active");
    expect(domainState({ ...base, autorenew: false, expiresAt: new Date("2026-11-20T00:00:00Z") }, ASOF)).toBe("expiring");
    expect(domainState({ ...base, absentSince: new Date("2026-11-04T12:50:00Z") }, ASOF)).toBe("gone");
    expect(domainState({ ...base, expiresAt: new Date("2026-11-01T00:00:00Z") }, ASOF)).toBe("expired");
  });
});

describe("item 14: a cancelled domain is tagged as one", () => {
  it("retiring = cancelled at the vendor, removed at the end of the month", () => {
    expect(toExclusion("retiring", "no new sequences from 2026-10-21", ASOF)).toEqual({
      reason: "retiring", note: "no new sequences from 2026-10-21", state: "cancelled", removedAt: "2026-12-01T00:00:00.000Z",
    });
    expect(toExclusion("brand", null, ASOF).state).toBe("brand");
    expect(endOfMonthUtc(new Date("2026-12-31T23:00:00Z"))).toBe("2027-01-01T00:00:00.000Z");
  });
});

describe("item 6: a mailbox on a gone domain is gone", () => {
  it("boostdistribute: domain gone, no credential, address still present in the old snapshot", () => {
    const { mailboxes } = deriveMailboxes({
      credentials: [],
      accounts: [{ email: "kevin@boostdistribute.com", timestampCreated: null, vendorPrewarmedAt: null, absentSince: null }],
      vendorMailboxes: [],
      domainProviders: [],
      goneDomains: [{ domain: "boostdistribute.com", goneSince: new Date("2026-11-04T12:50:30Z") }],
    });
    expect(mailboxes[0]!.absentSince).toEqual(new Date("2026-11-04T12:50:30Z"));
  });
  it("keeps a mailbox we still hold a credential for", () => {
    const { mailboxes } = deriveMailboxes({
      credentials: [{ address: "kevin@x.com", login: "kevin@x.com", source: "manual" }],
      accounts: [{ email: "kevin@x.com", timestampCreated: null, vendorPrewarmedAt: null, absentSince: null }],
      vendorMailboxes: [],
      domainProviders: [],
      goneDomains: [{ domain: "x.com", goneSince: new Date("2026-11-04T12:50:30Z") }],
    });
    expect(mailboxes[0]!.absentSince).toBeNull();
  });
});

describe("item 9: one inbox bar", () => {
  it("the placement alert reads the production bar", () => {
    expect(PLACEMENT_ALERT_INBOX_PCT_BAR).toBe(PRODUCTION_DELIVERY_PCT_BAR);
  });
});

describe("item 11: the capacity verdict is served", () => {
  const step = (account: string, at: string, isFirstEmail = false) => ({ account, isFirstEmail, theoreticalAt: at, overdue: true });
  it("red when a follow-up is more than a day late or sits on a mailbox that cannot send", () => {
    const v = capacityVerdict({
      steps: [
        step("a@x.com", "2026-10-29T10:00:00Z"),
        step("a@x.com", "2026-11-05T10:00:00Z"),
        step("gone@x.com", "2026-11-08T10:00:00Z"),
        step("r@x.com", "2026-11-04T10:00:00Z", true),
      ],
      asOf: ASOF,
      cannotSend: new Set(["gone@x.com"]),
      lifecycleOf: new Map([["a@x.com", "in_production"], ["r@x.com", "in_recovery"]]),
      dailyCapacity: 1614,
    });
    expect(v).toMatchObject({
      status: "broken",
      overdueFollowUps: 2,
      followUpsLateOverOneDay: 1,
      oldestOverdueFollowUpAt: "2026-10-29T10:00:00.000Z",
      overdueFirstEmails: 1,
      strandedSteps: 1,
      queuedOnNonProduction: 1,
    });
  });
  it("amber when only yesterday's follow-ups wait, green when nothing is late", () => {
    const base = { asOf: ASOF, cannotSend: new Set<string>(), lifecycleOf: new Map([["a@x.com", "in_production"]]), dailyCapacity: 100 };
    expect(capacityVerdict({ ...base, steps: [step("a@x.com", "2026-11-05T10:00:00Z")] }).status).toBe("behind");
    expect(capacityVerdict({ ...base, steps: [step("a@x.com", "2026-11-09T10:00:00Z")] }).status).toBe("ok");
  });
});
