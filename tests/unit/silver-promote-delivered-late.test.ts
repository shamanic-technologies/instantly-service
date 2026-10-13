import { describe, it, expect, vi, beforeEach } from "vitest";

// A reply recovered long after it was written (forwarded-replies.ts) records
// its fact but must not act as if the prospect had just written: no
// celebration, no responder run, no follow-up queue, no hand-over, no apology,
// no call to the rep. The decline/opt-out halves still run.

const mockDbSelect = vi.fn();
const mockDbInsertReturning = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: () => ({ where: mockDbSelect }) }),
    insert: () => ({
      values: () => ({ onConflictDoNothing: () => ({ returning: () => mockDbInsertReturning() }) }),
    }),
    update: () => ({ set: () => ({ where: () => Promise.resolve([{}]) }) }),
    execute: () => Promise.resolve({ rows: [] }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { instantlyCampaignId: "instantly_campaign_id" },
  instantlyEvents: { id: "id", campaignId: "campaign_id", leadEmail: "lead_email", eventType: "event_type", step: "step", inferred: "inferred" },
  sequenceCosts: { id: "id", campaignId: "campaign_id", leadEmail: "lead_email", step: "step", status: "status" },
}));

const live = {
  trigger: vi.fn(),
  enqueue: vi.fn(),
  forward: vi.fn(),
  escalate: vi.fn(),
  notAProspect: vi.fn(),
  ringRep: vi.fn(),
};
const decline = vi.fn();
const optOut = vi.fn();

vi.mock("../../src/lib/trigger-sales-interest-campaign", () => ({
  maybeTriggerSalesInterestCampaign: (...a: unknown[]) => live.trigger(...a),
}));
vi.mock("../../src/lib/enqueue-followup-on-interest", () => ({
  maybeEnqueueFollowupOnInterest: (...a: unknown[]) => live.enqueue(...a),
}));
vi.mock("../../src/lib/forward-positive-reply", () => ({
  maybeForwardPositiveReply: (...a: unknown[]) => live.forward(...a),
}));
vi.mock("../../src/lib/escalate-off-topic-reply", () => ({
  maybeEscalateOffTopicReply: (...a: unknown[]) => live.escalate(...a),
}));
vi.mock("../../src/lib/not-a-prospect", () => ({
  maybeHandleNotAProspect: (...a: unknown[]) => live.notAProspect(...a),
}));
vi.mock("../../src/lib/ring-rep-on-sales-interest", () => ({
  maybeRingRepOnSalesInterest: (...a: unknown[]) => live.ringRep(...a),
}));
vi.mock("../../src/lib/stop-followups-on-decline", () => ({
  maybeStopFollowupsOnDecline: (...a: unknown[]) => decline(...a),
}));
vi.mock("../../src/lib/reply-opt-out", () => ({
  maybeRecordOptOutFromReply: (...a: unknown[]) => optOut(...a),
}));
const mirror = vi.fn();
vi.mock("../../src/lib/mirror-emails", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  maybeMirrorCampaignEmails: (...a: unknown[]) => mirror(...a),
}));
vi.mock("../../src/lib/refine-interest-kind", () => ({
  refineInstantlyInterest: (_c: unknown, input: { eventType: string }) =>
    Promise.resolve({ eventType: input.eventType, reading: null }),
}));
vi.mock("../../src/lib/status-gold", () => ({ refreshLeadStatusCurrent: vi.fn() }));
vi.mock("../../src/lib/evidence-changed", () => ({ announceEvidenceChanged: vi.fn() }));
vi.mock("../../src/lib/runs-client", () => ({ updateCostStatus: vi.fn(), isRunGoneError: () => false }));

import { promoteEvent } from "../../src/lib/silver-promote";

function arrange(): void {
  mockDbSelect.mockResolvedValueOnce([
    { campaignId: "camp-1", instantlyCampaignId: "inst-1", orgId: "org-1", userId: "u-1", runId: "r-1" },
  ]);
  mockDbInsertReturning.mockResolvedValueOnce([{ id: "silver-1" }]);
}

const base = {
  eventType: "lead_interested",
  instantlyCampaignId: "inst-1",
  leadEmail: "laura@fusionmedcare.com",
  accountEmail: "klourd@pressbeat.ai",
  step: 1,
  variant: null,
  timestamp: new Date("2026-08-18T16:42:16Z"),
  source: "self_send" as const,
  sourceRowId: "imap-row-1",
};

describe("promoteEvent: a reply delivered late", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDbSelect.mockResolvedValue([]);
    mockDbInsertReturning.mockResolvedValue([]);
    for (const fn of Object.values(live)) fn.mockResolvedValue(undefined);
    decline.mockResolvedValue(undefined);
    optOut.mockResolvedValue(undefined);
  });

  it("records the fact without acting as if they had just written", async () => {
    arrange();
    const result = await promoteEvent({ ...base, deliveredLate: true });
    expect(result.promoted).toBe(true);
    for (const fn of Object.values(live)) expect(fn).not.toHaveBeenCalled();
    expect(mirror).not.toHaveBeenCalled();
    expect(decline).toHaveBeenCalled();
    expect(optOut).toHaveBeenCalled();
  });

  it("control: the same reply delivered on time runs every live side effect", async () => {
    arrange();
    await promoteEvent(base);
    expect(live.trigger).toHaveBeenCalled();
    expect(live.enqueue).toHaveBeenCalled();
    expect(live.forward).toHaveBeenCalled();
    expect(live.ringRep).toHaveBeenCalled();
    expect(mirror).toHaveBeenCalled();
  });
});
