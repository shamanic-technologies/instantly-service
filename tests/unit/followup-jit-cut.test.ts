import { describe, it, expect, vi } from "vitest";

// FOLLOWUP-JIT-1107: the pure halves of the one-time follow-up cut. The DB-backed
// proof (holds cancelled, step 1 untouched, re-run idempotent) is
// tests/integration/followup-jit-cut.test.ts.
vi.mock("../../src/db", () => ({ db: {} }));

const { groupBySequence, summarizeByBrand } = await import("../../src/lib/followup-jit-cut");

function hold(icid: string, step: number, over: Partial<Record<string, unknown>> = {}) {
  return {
    holdId: `${icid}-${step}`,
    instantlyCampaignId: icid,
    campaignId: "camp",
    orgId: "org-1",
    userId: "user-1",
    brandIds: ["brand-a"],
    leadEmail: `${icid}@lead.test`,
    step,
    runId: `run-${icid}-${step}`,
    costId: "c",
    domainCostId: "d",
    ...over,
  };
}

describe("groupBySequence", () => {
  it("groups follow-up holds per sequence and never lets a first email through", () => {
    const seqs = groupBySequence([hold("self:a", 1), hold("self:a", 2), hold("self:a", 3), hold("self:b", 2)]);
    expect(seqs.map((s) => [s.instantlyCampaignId, s.holds.map((h) => h.step)])).toEqual([
      ["self:a", [2, 3]],
      ["self:b", [2]],
    ]);
  });
});

describe("summarizeByBrand", () => {
  it("counts leads, steps and dollars per org x brand set, most money first", () => {
    const seqs = groupBySequence([
      hold("self:a", 2), hold("self:a", 3),
      hold("self:b", 2, { brandIds: ["brand-b"] }),
      hold("self:c", 2, { brandIds: ["brand-a"] }),
    ]);
    const cents = new Map([["self:a", 5], ["self:b", 7.5], ["self:c", 2.5]]);
    expect(summarizeByBrand(seqs, cents)).toEqual([
      { orgId: "org-1", brandIds: ["brand-a"], leads: 2, steps: 3, usdCents: 7.5 },
      { orgId: "org-1", brandIds: ["brand-b"], leads: 1, steps: 1, usdCents: 7.5 },
    ]);
  });
});
