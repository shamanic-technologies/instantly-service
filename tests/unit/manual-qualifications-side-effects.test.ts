import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbInsertValues = vi.fn();
const mockDbUpdateSet = vi.fn();
const mockRefreshLeadStatusCurrent = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    // The side effects read the campaign row back to hand the forward and the
    // sales-interest trigger the caller campaign / user / run they need.
    select: () => ({
      from: () => ({
        where: () =>
          Promise.resolve([
            {
              instantlyCampaignId: "inst-1",
              campaignId: "camp-1",
              orgId: "org-1",
              userId: "user-1",
              runId: "run-1",
              brandIds: ["brand-1"],
            },
          ]),
      }),
    }),
    insert: () => ({
      values: (v: unknown) => {
        mockDbInsertValues(v);
        return Promise.resolve([{}]);
      },
    }),
    update: () => ({
      set: (v: unknown) => {
        mockDbUpdateSet(v);
        return { where: () => Promise.resolve([{}]) };
      },
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { instantlyCampaignId: "instantly_campaign_id" },
  instantlyEvents: { campaignId: "campaign_id", eventType: "event_type" },
  instantlyManualQualificationsRaw: {
    orgId: "org_id",
    instantlyCampaignId: "instantly_campaign_id",
    leadEmail: "lead_email",
    qualifiedAt: "qualified_at",
  },
}));

const mockPromoteEvent = vi.fn();

vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...args: unknown[]) => mockPromoteEvent(...args),
}));

vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...args: unknown[]) => mockRefreshLeadStatusCurrent(...args),
}));

const mockResolveInstantlyApiKey = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

const mockForwardPositiveReply = vi.fn();
const mockTriggerSalesInterest = vi.fn();

vi.mock("../../src/lib/forward-positive-reply", () => ({
  maybeForwardPositiveReply: (...args: unknown[]) => mockForwardPositiveReply(...args),
}));

vi.mock("../../src/lib/trigger-sales-interest-campaign", () => ({
  maybeTriggerSalesInterestCampaign: (...args: unknown[]) => mockTriggerSalesInterest(...args),
}));

import {
  applyManualQualificationSideEffects,
  isSequenceStoppingQualification,
  MANUAL_QUALIFICATION_STATUSES,
  type ManualQualificationStatus,
} from "../../src/lib/manual-qualifications";
import { resolveReplyKind } from "../../src/lib/reply-kind";

beforeEach(() => {
  vi.resetAllMocks();
  mockPromoteEvent.mockResolvedValue({ promoted: true, silverEventId: "ev-1" });
  mockRefreshLeadStatusCurrent.mockResolvedValue(undefined);
  mockResolveInstantlyApiKey.mockResolvedValue({ key: "inst-key" });
  mockUpdateCampaignStatus.mockResolvedValue(undefined);
});

describe("applyManualQualificationSideEffects", () => {
  const baseInput = {
    bronzeRowId: "bronze-1",
    orgId: "org-1",
    instantlyCampaignId: "inst-camp-1",
    leadEmail: "lead@test.com",
    qualifiedAt: new Date("2026-05-24T10:00:00.000Z"),
    rawPayload: { campaign_id: "camp-1", email: "lead@test.com" },
  };

  /** Build the side-effect input the way the route does: resolve at write. */
  const inputFor = (status: ManualQualificationStatus) => ({
    ...baseInput,
    status,
    replyKind: resolveReplyKind(status),
  });

  it("synthesizes a `reply_received` silver event via promoteEvent (source='manual', inferred=false)", async () => {
    await applyManualQualificationSideEffects(inputFor("lead_interested"));

    expect(mockPromoteEvent).toHaveBeenCalledTimes(1);
    const call = mockPromoteEvent.mock.calls[0][0];
    expect(call).toEqual(
      expect.objectContaining({
        eventType: "reply_received",
        instantlyCampaignId: "inst-camp-1",
        leadEmail: "lead@test.com",
        source: "manual",
        inferred: false,
        sourceRowId: "bronze-1",
      }),
    );
  });

  // The RESOLVED kind reaches silver, never the raw legacy statement — that is
  // what keeps every reader on the new vocabulary with no read-time map.
  it("writes the RESOLVED reply kind in silver for a legacy deal-progress statement", async () => {
    await applyManualQualificationSideEffects(inputFor("lead_meeting_booked"));

    expect(
      mockDbInsertValues.mock.calls.some(
        (c) => (c[0] as Record<string, unknown>).eventType === "lead_meeting_booked",
      ),
    ).toBe(false);

    const leadStatusInsert = mockDbInsertValues.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.eventType === "lead_interested";
    });
    expect(leadStatusInsert).toBeDefined();
    const v = leadStatusInsert![0] as Record<string, unknown>;
    expect(v.source).toBe("manual");
    expect(v.inferred).toBe(false);
    expect(v.campaignId).toBe("inst-camp-1");
    expect(v.leadEmail).toBe("lead@test.com");
  });

  it("pins reply_classification + source='manual' on instantly_campaigns", async () => {
    await applyManualQualificationSideEffects(inputFor("lead_interested"));

    const updateCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return "replyClassification" in v;
    });
    expect(updateCall).toBeDefined();
    const v = updateCall![0] as Record<string, unknown>;
    expect(v.replyClassification).toBe("positive");
    expect(v.replyClassificationSource).toBe("manual");
  });

  it("refreshes the Gold status row after pinning the manual classification", async () => {
    await applyManualQualificationSideEffects(inputFor("lead_not_interested"));

    expect(mockRefreshLeadStatusCurrent).toHaveBeenCalledWith(
      "inst-camp-1",
      "lead@test.com",
    );
  });

  it("maps lead_not_interested to negative", async () => {
    await applyManualQualificationSideEffects(inputFor("lead_not_interested"));

    const updateCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return "replyClassification" in v;
    });
    expect(updateCall).toBeDefined();
    expect((updateCall![0] as Record<string, unknown>).replyClassification).toBe("negative");
  });

  it("maps lead_neutral / lead_out_of_office / auto_reply_received to neutral", async () => {
    for (const status of ["lead_neutral", "lead_out_of_office", "auto_reply_received"] as const) {
      vi.resetAllMocks();
      mockPromoteEvent.mockResolvedValue({ promoted: true, silverEventId: "ev-1" });

      await applyManualQualificationSideEffects(inputFor(status));

      const updateCall = mockDbUpdateSet.mock.calls.find((c) => {
        const v = c[0] as Record<string, unknown>;
        return "replyClassification" in v;
      });
      expect(updateCall).toBeDefined();
      expect((updateCall![0] as Record<string, unknown>).replyClassification).toBe("neutral");
    }
  });

  // ── Stopping the sequence on BOTH sides ───────────────────────────────────
  //
  // A manual qualification exists because Instantly did NOT detect the reply,
  // so its own stop-on-reply can never fire. Cancelling the local cost holds
  // (what the synthesized reply_received does) refunds the spend but tells
  // Instantly nothing — without the pause it keeps dispatching the remaining
  // steps to a prospect who already answered.

  it("PAUSES the lead's Instantly campaign on a sequence-stopping qualification", async () => {
    await applyManualQualificationSideEffects(inputFor("lead_interested"));

    expect(mockResolveInstantlyApiKey).toHaveBeenCalledWith(
      "org-1",
      "system",
      expect.objectContaining({ method: "POST" }),
    );
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith(
      "inst-key",
      "inst-camp-1",
      "paused",
    );
  });

  it("pauses for every sequence-stopping status", async () => {
    for (const status of [
      "lead_interested",
      "lead_meeting_booked",
      "lead_closed",
      "lead_not_interested",
      "lead_wrong_person",
      "lead_neutral",
    ] as const) {
      vi.resetAllMocks();
      mockPromoteEvent.mockResolvedValue({ promoted: true, silverEventId: "ev-1" });
      mockResolveInstantlyApiKey.mockResolvedValue({ key: "inst-key" });

      await applyManualQualificationSideEffects(inputFor(status));

      expect(isSequenceStoppingQualification(status)).toBe(true);
      expect(mockUpdateCampaignStatus).toHaveBeenCalledWith(
        "inst-key",
        "inst-camp-1",
        "paused",
      );
    }
  });

  // An autoresponder is NOT a reply (RFC 3834) — the prospect is back next
  // week and has not engaged. Stopping would end the outreach AND refund the
  // spend for someone who never answered. The predicate gates BOTH the
  // reply_received synthesis (which cancels the holds) and the pause, so the
  // two sides can never contradict each other.
  it("does NOT stop the sequence for an autoresponder (out_of_office / auto_reply_received)", async () => {
    for (const status of ["lead_out_of_office", "auto_reply_received"] as const) {
      vi.resetAllMocks();
      mockResolveInstantlyApiKey.mockResolvedValue({ key: "inst-key" });

      await applyManualQualificationSideEffects(inputFor(status));

      expect(isSequenceStoppingQualification(status)).toBe(false);
      // No synthesized reply ⇒ the lead's provisioned holds are NOT cancelled.
      expect(mockPromoteEvent).not.toHaveBeenCalled();
      // …and Instantly keeps sending, which is the point.
      expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();

      // The qualification itself is still recorded in silver + pinned.
      const leadStatusInsert = mockDbInsertValues.mock.calls.find((c) => {
        const v = c[0] as Record<string, unknown>;
        return v.eventType === status;
      });
      expect(leadStatusInsert).toBeDefined();
      expect(mockRefreshLeadStatusCurrent).toHaveBeenCalled();
    }
  });

  it("every manual status is classified as stopping or not (no status left unhandled)", () => {
    for (const status of MANUAL_QUALIFICATION_STATUSES) {
      expect(typeof isSequenceStoppingQualification(status)).toBe("boolean");
    }
    const stopping = MANUAL_QUALIFICATION_STATUSES.filter(isSequenceStoppingQualification);
    // 4 positive + 3 negative + lead_neutral + the 2 still-accepted legacy
    // deal-progress values (which resolve to a positive kind).
    expect(stopping).toHaveLength(10);
    // A person stating they changed job HAS replied — the sequence stops, and
    // its remaining holds are refunded, exactly like any other human reply.
    expect(stopping).toContain("lead_changed_job");
    expect(stopping).not.toContain("lead_out_of_office");
    expect(stopping).not.toContain("auto_reply_received");
  });

  // Fail-soft: the bronze row is already committed when this runs, so a pause
  // failure must not 500 a qualification that did land.
  it("swallows an Instantly pause failure and still pins the classification", async () => {
    mockUpdateCampaignStatus.mockRejectedValue(new Error("instantly 500"));

    await expect(
      applyManualQualificationSideEffects(inputFor("lead_interested")),
    ).resolves.toBeUndefined();

    const updateCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return "replyClassification" in v;
    });
    expect(updateCall).toBeDefined();
    expect((updateCall![0] as Record<string, unknown>).replyClassification).toBe("positive");
  });

  it("swallows a key-resolution failure", async () => {
    mockResolveInstantlyApiKey.mockRejectedValue(new Error("key-service down"));

    await expect(
      applyManualQualificationSideEffects(inputFor("lead_closed")),
    ).resolves.toBeUndefined();

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(mockRefreshLeadStatusCurrent).toHaveBeenCalled();
  });
});

/**
 * A manual qualification is created PRECISELY because Instantly missed the
 * reply, so it is the case that needs the positive-reply consequences most —
 * and until this was wired it was the one case that got neither. Step 2 inserts
 * the reply-kind event directly (to pin `reply_classification_source='manual'`),
 * which bypasses `promoteEvent` and with it both side effects; the
 * `reply_received` that does go through `promoteEvent` is in neither positive
 * set. Net effect: a human recording "this prospect is interested" reached
 * nobody. Observed in prod on a real `lead_interested` statement whose
 * `positive_reply_forwarded_at` stayed null.
 */
describe("applyManualQualificationSideEffects — a POSITIVE manual statement reaches the agency inbox", () => {
  it("forwards the thread and asks the funded campaign to run", async () => {
    await applyManualQualificationSideEffects({
      bronzeRowId: "bronze-1",
      orgId: "org-1",
      instantlyCampaignId: "inst-1",
      leadEmail: "lead@test.com",
      status: "lead_interested",
      replyKind: "lead_interested",
      qualifiedAt: new Date("2026-09-03T10:00:00Z"),
      rawPayload: {},
    });

    expect(mockForwardPositiveReply).toHaveBeenCalledWith(
      expect.objectContaining({ instantlyCampaignId: "inst-1", campaignId: "camp-1" }),
      "lead@test.com",
      "lead_interested",
    );
    expect(mockTriggerSalesInterest).toHaveBeenCalledWith(
      expect.objectContaining({ instantlyCampaignId: "inst-1", campaignId: "camp-1" }),
      "lead@test.com",
      "lead_interested",
    );
  });

  it("hands them the reply KIND, not the raw legacy statement", async () => {
    // `lead_closed` resolves to `lead_interested`; the side effects gate on the
    // vocabulary, so passing the raw status through would silently no-op.
    await applyManualQualificationSideEffects({
      bronzeRowId: "bronze-1",
      orgId: "org-1",
      instantlyCampaignId: "inst-1",
      leadEmail: "lead@test.com",
      status: "lead_closed",
      replyKind: resolveReplyKind("lead_closed"),
      qualifiedAt: new Date("2026-09-03T10:00:00Z"),
      rawPayload: {},
    });

    expect(mockForwardPositiveReply).toHaveBeenCalledWith(
      expect.anything(),
      "lead@test.com",
      "lead_interested",
    );
  });

  it("still calls them for a NEGATIVE kind — each one self-gates on the event type", async () => {
    // Deliberately not gated here: both helpers already refuse a non-positive
    // kind, and duplicating that decision would let the two definitions drift.
    await applyManualQualificationSideEffects({
      bronzeRowId: "bronze-1",
      orgId: "org-1",
      instantlyCampaignId: "inst-1",
      leadEmail: "lead@test.com",
      status: "lead_not_interested",
      replyKind: "lead_not_interested",
      qualifiedAt: new Date("2026-09-03T10:00:00Z"),
      rawPayload: {},
    });

    expect(mockForwardPositiveReply).toHaveBeenCalledWith(
      expect.anything(),
      "lead@test.com",
      "lead_not_interested",
    );
  });
});
