import { describe, it, expect } from "vitest";

import {
  BounceBudget,
  bounceBudgetRefusal,
  senderDomain,
  type DomainBounceHistory,
  type FleetBounceHistory,
} from "../../src/lib/bounce-budget";
import {
  selectDueSteps,
  type AccountCapacity,
  type PendingSequence,
} from "../../src/lib/self-send/dispatch";

function history(day: [number, number], fortnight: [number, number] = day): DomainBounceHistory {
  return new Map([
    [24, { sends: day[0], bounces: day[1] }],
    [336, { sends: fortnight[0], bounces: fortnight[1] }],
  ]);
}

describe("bounce budget: the rule (CATCHALL-BOUNCE-GUARD-1015)", () => {
  it("a valid address is never gated by the risk budget, even on a domain with no volume", () => {
    expect(bounceBudgetRefusal(undefined, "valid")).toBeNull();
    expect(bounceBudgetRefusal(history([3, 1]), "valid")).toBeNull();
  });

  it("a catch-all waits for clean volume: 3 sends cannot carry it, 10 clean sends can", () => {
    expect(bounceBudgetRefusal(history([3, 0]), "catch_all")).toBe("risk_budget");
    // 0 + 0.107 <= 0.02 * (10 + 1) = 0.22
    expect(bounceBudgetRefusal(history([10, 0], [200, 1]), "catch_all")).toBeNull();
  });

  it("a domain that just bounced takes no catch-all until diluted", () => {
    // 1 bounce on 40 sends today: 1.107 > 0.82
    expect(bounceBudgetRefusal(history([40, 1], [400, 1]), "catch_all")).toBe("risk_budget");
  });

  it("the 14-day window remembers a domain that burned this fortnight", () => {
    // clean today, 9 bounces on 300 over 14 days: 9.107 > 6.02
    expect(bounceBudgetRefusal(history([60, 0], [300, 9]), "catch_all")).toBe("risk_budget");
  });

  it("an unknown verdict, or none, costs far more than a catch-all", () => {
    const h = history([30, 0], [300, 0]);
    expect(bounceBudgetRefusal(h, "catch_all")).toBeNull();
    expect(bounceBudgetRefusal(h, "unknown")).toBe("risk_budget");
    expect(bounceBudgetRefusal(h, null)).toBe("risk_budget");
  });

  it("hard ceiling: >= 5 % over 14 days with >= 100 sends holds VALID first emails too", () => {
    expect(bounceBudgetRefusal(history([50, 0], [120, 6]), "valid")).toBe("hard_ceiling");
  });

  it("hard ceiling needs a sample: 3 bounces on 20 sends is not a judgment", () => {
    expect(bounceBudgetRefusal(history([20, 3], [20, 3]), "valid")).toBeNull();
  });

  it("BounceBudget spends in memory: a tick cannot plan a burst of catch-alls on one domain", () => {
    const fleet: FleetBounceHistory = new Map([["boost.com", history([30, 0], [300, 1])]]);
    const budget = new BounceBudget(fleet);
    let planned = 0;
    for (let i = 0; i < 60; i += 1) {
      if (budget.refusal("kevin@boost.com", "catch_all") !== null) continue;
      budget.spend("kevin@boost.com", "catch_all", true);
      planned += 1;
    }
    // 0.02 * (30 + n + 1) >= 0 + 0.107 * (n + 1) holds for n <= 6
    expect(planned).toBeLessThanOrEqual(6);
    expect(planned).toBeGreaterThan(0);
    // another domain is untouched
    expect(senderDomain("Kevin@Other.com")).toBe("other.com");
  });
});

// Monday 10:00 America/Chicago, inside the default window.
const NOW = new Date("2026-08-17T15:00:00Z");

function seq(over: Partial<PendingSequence> = {}): PendingSequence {
  return {
    instantlyCampaignId: "camp-1",
    leadEmail: "p@x.com",
    accountEmail: "kevin@boost.com",
    provisionedSteps: [1, 2, 3],
    lastSentStep: null,
    lastSentAt: null,
    stepDelays: [3, 7, null],
    verdict: "catch_all",
    ...over,
  };
}

function cap(accountEmail: string, over: Partial<AccountCapacity> = {}): AccountCapacity {
  return {
    accountEmail,
    mailbox: accountEmail,
    cap: 50,
    recentSustainedDaily: 50,
    sentToday: 0,
    adoptsFirstEmails: true,
    ...over,
  };
}

describe("a catch-all lead aimed at a domain without budget stays queued, then is sent", () => {
  const burnt = history([40, 3], [300, 6]);
  const clean = history([60, 0], [500, 2]);

  it("is held, not dropped, when no domain can afford it", () => {
    const budget = new BounceBudget(new Map([["boost.com", burnt]]));
    const out = selectDueSteps([seq()], [cap("kevin@boost.com")], NOW, new Set(), budget);
    expect(out.selected).toEqual([]);
    expect(out.heldBounceBudget).toBe(1);
    expect(out.dueBeforeCapacity).toBe(1);
  });

  it("is sent from a healthy domain's mailbox as soon as one has room (rehome)", () => {
    const budget = new BounceBudget(
      new Map([
        ["boost.com", burnt],
        ["clean.com", clean],
      ]),
    );
    const out = selectDueSteps(
      [seq()],
      [cap("kevin@boost.com"), cap("ana@clean.com")],
      NOW,
      new Set(),
      budget,
    );
    expect(out.selected).toHaveLength(1);
    expect(out.selected[0]).toMatchObject({ accountEmail: "ana@clean.com", rehomedFrom: "kevin@boost.com" });
    expect(out.heldBounceBudget).toBe(1);
    expect(out.rehomed).toBe(1);
  });

  it("is sent from its own mailbox once its domain is back under budget (next run)", () => {
    const recovered = new BounceBudget(new Map([["boost.com", history([60, 0], [500, 3])]]));
    const out = selectDueSteps([seq()], [cap("kevin@boost.com")], NOW, new Set(), recovered);
    expect(out.selected).toHaveLength(1);
    expect(out.selected[0]!.accountEmail).toBe("kevin@boost.com");
    expect(out.heldBounceBudget).toBe(0);
  });

  it("a valid lead on the same burnt domain still goes out (no hard ceiling reached)", () => {
    const budget = new BounceBudget(new Map([["boost.com", burnt]]));
    const out = selectDueSteps([seq({ verdict: "valid" })], [cap("kevin@boost.com")], NOW, new Set(), budget);
    expect(out.selected).toHaveLength(1);
  });

  it("a follow-up, or an address that already accepted our email, spends no budget", () => {
    const budget = new BounceBudget(new Map([["boost.com", burnt]]));
    const followUp = seq({
      instantlyCampaignId: "fu",
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date(NOW.getTime() - 5 * 86_400_000),
    });
    const proven = seq({ instantlyCampaignId: "proven", leadEmail: "q@x.com", addressProven: true });
    const out = selectDueSteps([followUp, proven], [cap("kevin@boost.com")], NOW, new Set(), budget);
    expect(out.selected.map((s) => s.instantlyCampaignId).sort()).toEqual(["fu", "proven"]);
  });

  it("CONTROL: without a budget the selection behaves exactly as before", () => {
    const out = selectDueSteps([seq()], [cap("kevin@boost.com")], NOW);
    expect(out.selected).toHaveLength(1);
    expect(out.heldBounceBudget).toBe(0);
  });
});
