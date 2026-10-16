/**
 * DOMAIN-BLOCKLIST-1016: a sending domain on a public blocklist takes no new
 * sequence, its follow-ups keep going out, and it comes back on a fresh clean
 * answer. A refused query is never a listing nor a clean bill.
 */
import { describe, it, expect, vi } from "vitest";

vi.mock("../../src/db", () => ({ db: {} }));

import {
  BLOCKLISTS,
  blocklistByKey,
  planTransitions,
  readLookup,
  type CurrentState,
} from "../../src/lib/domain-blocklist";
import { checkList, type Lookup } from "../../src/lib/domain-blocklist-sync";
import { buildBlocklistAlert } from "../../src/lib/domain-blocklist-alert";
import { buildListingSeries, windowDays } from "../../src/lib/domain-blocklist-read";
import { deriveLifecycle, type DeriveLifecycleInput } from "../../src/lib/account-lifecycle";
import { selectDueSteps, type AccountCapacity, type PendingSequence } from "../../src/lib/self-send/dispatch";

const surbl = blocklistByKey("surbl")!;
const dbl = blocklistByKey("spamhaus_dbl")!;
const uribl = blocklistByKey("uribl")!;

describe("reading a list's answer", () => {
  it("SURBL 127.0.0.64 is the ABUSE list", () => {
    expect(readLookup(surbl, { kind: "answer", addresses: ["127.0.0.64"] })).toEqual({
      outcome: "listed",
      answer: "127.0.0.64",
      labels: ["ABUSE"],
    });
  });

  it("NXDOMAIN / NODATA is clean", () => {
    expect(readLookup(surbl, { kind: "nxdomain" })).toEqual({ outcome: "clean" });
  });

  it("a refusal is never a listing: URIBL 127.0.0.1, Spamhaus 127.255.255.254, SURBL 127.0.0.1", () => {
    expect(readLookup(uribl, { kind: "answer", addresses: ["127.0.0.1"] }).outcome).toBe("unanswered");
    expect(readLookup(dbl, { kind: "answer", addresses: ["127.255.255.254"] }).outcome).toBe("unanswered");
    expect(readLookup(surbl, { kind: "answer", addresses: ["127.0.0.1"] }).outcome).toBe("unanswered");
  });

  it("a resolver error is unanswered, never clean", () => {
    expect(readLookup(surbl, { kind: "error", code: "ETIMEOUT" })).toEqual({ outcome: "unanswered", error: "ETIMEOUT" });
  });

  it("Spamhaus DBL 127.0.1.2 is SPAM; URIBL grey alone is not a listing", () => {
    expect(readLookup(dbl, { kind: "answer", addresses: ["127.0.1.2"] })).toMatchObject({ outcome: "listed", labels: ["SPAM"] });
    expect(readLookup(uribl, { kind: "answer", addresses: ["127.0.0.4"] }).outcome).toBe("clean");
    expect(readLookup(uribl, { kind: "answer", addresses: ["127.0.0.2"] })).toMatchObject({ outcome: "listed", labels: ["BLACK"] });
  });

  it("checks the three domain lists the market reads", () => {
    expect(BLOCKLISTS.map((l) => l.zone)).toEqual(["multi.surbl.org", "dbl.spamhaus.org", "multi.uribl.com"]);
  });
});

describe("the canary gates a whole list", () => {
  const fake = (answers: Record<string, string[] | "nx">): (() => Promise<Lookup>) =>
    async () => async (name) => {
      const a = answers[name];
      if (a === undefined || a === "nx") return { kind: "nxdomain" };
      return { kind: "answer", addresses: a };
    };

  it("canary answered as listed: each domain is read", async () => {
    const run = await checkList(
      surbl,
      ["grow.com", "clean.com"],
      fake({ "test.surbl.org.multi.surbl.org": ["127.0.0.254"], "grow.com.multi.surbl.org": ["127.0.0.64"] }),
    );
    expect(run.unanswered).toBeNull();
    expect(run.verdicts.map((v) => v.verdict.outcome)).toEqual(["listed", "clean"]);
  });

  it("canary NOT answered as listed (refused, or a resolver lying NXDOMAIN): every domain is unanswered", async () => {
    const run = await checkList(surbl, ["grow.com"], fake({ "grow.com.multi.surbl.org": "nx" }));
    expect(run.unanswered).toMatch(/canary/);
    expect(run.verdicts[0]!.verdict.outcome).toBe("unanswered");
  });

  it("nameservers unreachable: unanswered, never clean", async () => {
    const run = await checkList(dbl, ["grow.com"], async () => {
      throw new Error("no nameserver address");
    });
    expect(run.verdicts[0]!.verdict).toEqual({ outcome: "unanswered", error: "resolver: no nameserver address" });
  });
});

describe("a new listing writes one event; the same listing next day writes none", () => {
  const listed = { outcome: "listed" as const, answer: "127.0.0.64", labels: ["ABUSE"] };

  it("first seen listed: one `listed` transition", () => {
    expect(planTransitions([], [{ domain: "grow.com", list: "surbl", verdict: listed }])).toEqual([
      { domain: "grow.com", list: "surbl", type: "listed", answer: "127.0.0.64", labels: ["ABUSE"] },
    ]);
  });

  it("still listed the next day: nothing (so no second alert)", () => {
    const current: CurrentState[] = [{ domain: "grow.com", list: "surbl", listed: true, listedSince: new Date() }];
    expect(planTransitions(current, [{ domain: "grow.com", list: "surbl", verdict: listed }])).toEqual([]);
  });

  it("listed then a fresh clean answer: one `delisted`", () => {
    const current: CurrentState[] = [{ domain: "grow.com", list: "surbl", listed: true, listedSince: new Date() }];
    expect(planTransitions(current, [{ domain: "grow.com", list: "surbl", verdict: { outcome: "clean" } }])).toEqual([
      { domain: "grow.com", list: "surbl", type: "delisted", answer: null, labels: [] },
    ]);
  });

  it("listed and the list refused us today: stays listed (never marked clean unchecked)", () => {
    const current: CurrentState[] = [{ domain: "grow.com", list: "surbl", listed: true, listedSince: new Date() }];
    expect(
      planTransitions(current, [{ domain: "grow.com", list: "surbl", verdict: { outcome: "unanswered", error: "refused" } }]),
    ).toEqual([]);
  });

  it("a clean domain checked again clean: nothing", () => {
    const current: CurrentState[] = [{ domain: "ok.com", list: "surbl", listed: false, listedSince: null }];
    expect(planTransitions(current, [{ domain: "ok.com", list: "surbl", verdict: { outcome: "clean" } }])).toEqual([]);
  });
});

describe("the alert names the domain, list, date, mailboxes, what was done and the removal link", () => {
  it("renders from the event alone", () => {
    const alert = buildBlocklistAlert({
      id: "evt-1",
      domain: "agileconsultco.com",
      list: "surbl",
      answer: "127.0.0.64",
      labels: ["ABUSE"],
      mailboxes: ["raquel@agileconsultco.com", "ruth@agileconsultco.com"],
      detail: { firstBlocklistBounceAt: "2026-10-01T09:00:00.000Z" },
      occurredAt: new Date("2026-10-16T08:00:00.000Z"),
    });
    expect(alert).toMatchObject({
      eventId: "evt-1",
      domain: "agileconsultco.com",
      list: "SURBL",
      listedSince: "2026-10-16",
      listAnswer: "ABUSE (127.0.0.64)",
      mailboxes: "raquel@agileconsultco.com, ruth@agileconsultco.com",
      removalUrl: "https://www.surbl.org/surbl-analysis",
    });
    expect(alert.actionTaken).toMatch(/2 mailboxes take no new prospects/);
    expect(alert.actionTaken).toMatch(/Follow-ups already planned from them still go out/);
    expect(alert.actionTaken).toMatch(/2026-10-01/);
    expect(`${alert.actionTaken}${alert.listAnswer}`).not.toMatch(/[—–]/);
  });
});

describe("lifecycle: a listed domain is out of new sequences, a clean one comes back", () => {
  const base: DeriveLifecycleInput = {
    sendTransport: "smtp",
    instantlyStatus: 1,
    domain: "agileconsultco.com",
    healthScore: 100,
    deliveryAtBar: true,
    deliveryTestedAt: new Date("2026-10-10T00:00:00.000Z").toISOString(),
    currentStatus: "in_production",
    domainPolicy: new Set(["distribute.you"]),
    blocklistedDomains: new Set(["agileconsultco.com"]),
    asOf: new Date("2026-10-16T00:00:00.000Z"),
  };

  it("listed: in_recovery for `domain_blocklisted`, even from in_production with a passing test", () => {
    expect(deriveLifecycle(base)).toEqual({ status: "in_recovery", reason: "domain_blocklisted" });
  });

  it("clean again: back to production by itself", () => {
    expect(deriveLifecycle({ ...base, blocklistedDomains: new Set(), currentStatus: "in_recovery" })).toEqual({
      status: "in_production",
      reason: "passed",
    });
  });

  it("a brand domain stays the user's decision", () => {
    expect(
      deriveLifecycle({ ...base, domain: "distribute.you", blocklistedDomains: new Set(["distribute.you"]) }),
    ).toEqual({ status: "deactivated_by_user", reason: "brand_domain" });
  });
});

describe("dispatch: the listed mailbox sends follow-ups, never a first email", () => {
  // Monday 10:00 America/Chicago.
  const NOW = new Date("2026-08-17T15:00:00Z");
  const DAY = 86_400_000;
  const LISTED = "raquel@agileconsultco.com";
  const PROD = "amy@cleanco.com";

  const cap = (accountEmail: string, over: Partial<AccountCapacity> = {}): AccountCapacity => ({
    accountEmail,
    mailbox: accountEmail,
    cap: 50,
    recentSustainedDaily: 50,
    sentToday: 0,
    ...over,
  });
  const first: PendingSequence = {
    instantlyCampaignId: "first-1",
    leadEmail: "new@prospect.com",
    accountEmail: LISTED,
    provisionedSteps: [1, 2, 3],
    lastSentStep: null,
    lastSentAt: null,
    stepDelays: [3, 7, null],
  };
  const followUp: PendingSequence = {
    instantlyCampaignId: "fu-1",
    leadEmail: "old@prospect.com",
    accountEmail: LISTED,
    provisionedSteps: [2, 3],
    lastSentStep: 1,
    lastSentAt: new Date(NOW.getTime() - 5 * DAY),
    stepDelays: [3, 7, null],
  };

  it("listed: the follow-up goes out from its own mailbox, the first email moves to a production mailbox", () => {
    const out = selectDueSteps(
      [first, followUp],
      [cap(LISTED, { sendsFirstEmails: false }), cap(PROD, { adoptsFirstEmails: true })],
      NOW,
    );
    expect(out.heldBlocklisted).toBe(1);
    expect(out.selected).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ instantlyCampaignId: "fu-1", accountEmail: LISTED, step: 2 }),
        expect.objectContaining({ instantlyCampaignId: "first-1", accountEmail: PROD, rehomedFrom: LISTED }),
      ]),
    );
    expect(out.selected).toHaveLength(2);
  });

  it("listed and no production mailbox has room: the first email waits, it never leaves from the listed domain", () => {
    const out = selectDueSteps([first], [cap(LISTED, { sendsFirstEmails: false })], NOW);
    expect(out.selected).toEqual([]);
    expect(out.heldBlocklisted).toBe(1);
  });

  it("clean again: the mailbox sends its own first email", () => {
    const out = selectDueSteps([first], [cap(LISTED, { sendsFirstEmails: true })], NOW);
    expect(out.selected).toEqual([expect.objectContaining({ instantlyCampaignId: "first-1", accountEmail: LISTED })]);
    expect(out.heldBlocklisted).toBe(0);
  });
});

describe("daily series", () => {
  it("counts a domain on each day it was listed; days before monitoring are null, not zero", () => {
    const asOf = new Date("2026-10-20T12:00:00.000Z");
    const days = windowDays(asOf, 7);
    expect(days[0]).toBe("2026-10-14");
    const s = buildListingSeries(
      [
        { domain: "a.com", list: "surbl", type: "listed", at: new Date("2026-10-16T08:00:00Z"), mailboxes: ["x@a.com", "y@a.com"] },
        { domain: "a.com", list: "surbl", type: "delisted", at: new Date("2026-10-18T08:00:00Z"), mailboxes: [] },
        { domain: "b.com", list: "spamhaus_dbl", type: "listed", at: new Date("2026-10-19T08:00:00Z"), mailboxes: ["z@b.com"] },
      ],
      days,
      new Date("2026-10-16T08:00:00Z"),
      asOf,
    );
    expect(s.domainsListed.map((d) => d.total)).toEqual([null, null, 1, 1, 1, 1, 1]);
    expect(s.domainsListed[6]!.byList).toEqual({ surbl: 0, spamhaus_dbl: 1, uribl: 0 });
    expect(s.mailboxesOutOfNewSequences.map((d) => d.mailboxes)).toEqual([null, null, 2, 2, 2, 1, 1]);
  });
});
