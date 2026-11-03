import { describe, it, expect, vi, beforeEach } from "vitest";

// OLIVE-FOLLOWUPS-1103: a lead who got our first email gets every follow-up.
// The restore takes back, fleet-wide by one rule, the follow-ups cut while
// nothing about the lead said stop.

const mockExecute = vi.fn();
const mockRestoreOne = vi.fn();
const mockListCampaignStatuses = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockExecute(...a),
    insert: () => ({ values: () => ({ onConflictDoNothing: () => Promise.resolve() }) }),
  },
}));
vi.mock("../../src/lib/restore-stopped-followups", () => ({
  restoreOne: (...a: unknown[]) => mockRestoreOne(...a),
}));
vi.mock("../../src/lib/campaign-client", () => ({
  listCampaignStatuses: (...a: unknown[]) => mockListCampaignStatuses(...a),
}));
vi.mock("../../src/lib/campaign-identity", () => ({ identityKeyOf: () => null }));
vi.mock("../../src/lib/status-gold", () => ({ refreshLeadStatusCurrent: vi.fn(async () => {}) }));
vi.mock("../../src/lib/evidence-changed", () => ({ announceEvidenceChanged: vi.fn(async () => {}) }));

const { restoreCutFollowups, withoutTornDownOrgs, CUT_SINCE } = await import(
  "../../src/lib/restore-cut-followups"
);
const { retractBouncedEvents } = await import("../../src/lib/bounce-retraction");

const CALLER = { method: "POST", path: "/restore-cut-followups" };
const OLIVE = "308f528f-03ad-44b1-9cbc-623a6af9ba0e";

function row(id: string, over: Record<string, unknown> = {}) {
  return {
    rowId: `row-${id}`,
    instantlyCampaignId: `self:${id}`,
    campaignId: "804fac74-6f0d-4dc6-8f99-fc1e337dd08f",
    orgId: OLIVE,
    userId: "user-1",
    runId: "run-1",
    brandIds: ["c4b5284d-5add-440d-917e-0c79d920a0d4"],
    leadEmail: `${id}@flowdesk.co`,
    paused: true,
    steps: [2, 3],
    ...over,
  };
}

function sqlText(call: unknown[]): string {
  const query = call[0] as { queryChunks?: unknown[] };
  return JSON.stringify(query.queryChunks ?? query);
}

beforeEach(() => {
  vi.resetAllMocks();
  mockListCampaignStatuses.mockResolvedValue([]);
  mockRestoreOne.mockResolvedValue("restored");
});

describe("the population is one rule, never a list", () => {
  it("reads the lead's stop evidence: no reply, bounce, unsubscribe, click, reply kind, opt-out, manual qualification, follow-up cut, refused verdict, newer sequence", async () => {
    mockExecute.mockResolvedValue({ rows: [] });
    await restoreCutFollowups({ dryRun: true, caller: CALLER });
    const text = sqlText(mockExecute.mock.calls[0]!);
    for (const fragment of [
      "'reply_received', 'email_bounced', 'lead_unsubscribed', 'email_link_clicked'",
      "x.event_type LIKE 'lead",
      "instantly_lead_optouts_raw",
      "instantly_manual_qualifications_raw",
      "instantly_followup_cuts",
      "email_send_verifications",
      "n.created_at > s.created_at",
      "instantlyPlanRetired",
      "stalledFirstEmailClosed",
      "instantly_events_retracted",
    ]) {
      expect(text).toContain(fragment);
    }
    // Only steps AFTER the last real email, never re-sent, with a body to send.
    expect(text).toContain("sc.step > k.last_sent_step");
    expect(text).toContain("st.body_html <> ''");
    expect(CUT_SINCE).toBe("2026-10-07T00:00:00Z");
  });

  it("dry run: the plan per org, nothing restored", async () => {
    mockExecute.mockResolvedValue({
      rows: [row("a"), row("b", { steps: [3] }), row("c", { orgId: "other-org", steps: [2] })],
    });
    const summary = await restoreCutFollowups({ dryRun: true, caller: CALLER });
    expect(summary).toMatchObject({
      dryRun: true,
      candidates: 3,
      steps: 4,
      byOrg: { [OLIVE]: { leads: 2, steps: 3 }, "other-org": { leads: 1, steps: 1 } },
    });
    expect(mockRestoreOne).not.toHaveBeenCalled();
  });

  it("a campaign stopped for an org teardown is never restored; a payment stop or a customer stop is", async () => {
    mockExecute.mockResolvedValue({
      rows: [
        row("gone", { campaignId: "camp-teardown" }),
        row("unpaid", { campaignId: "camp-unpaid" }),
        row("manual", { campaignId: "camp-manual" }),
      ],
    });
    mockListCampaignStatuses.mockResolvedValue([
      { id: "camp-teardown", status: "stopped", stopReason: "org_teardown" },
      { id: "camp-unpaid", status: "stopped", stopReason: "payment_declined" },
      { id: "camp-manual", status: "stopped", stopReason: "manual" },
    ]);
    const summary = await restoreCutFollowups({ dryRun: false, caller: CALLER });
    expect(summary).toMatchObject({ candidates: 2, restored: 2, restoredSteps: 4, failed: 0 });
    expect(mockRestoreOne.mock.calls.map((c) => (c[0] as { leadEmail: string }).leadEmail)).toEqual([
      "unpaid@flowdesk.co",
      "manual@flowdesk.co",
    ]);
  });

  it("one failed lead is counted and the rest go on", async () => {
    mockExecute.mockResolvedValue({ rows: [row("a"), row("b")] });
    mockRestoreOne.mockRejectedValueOnce(new Error("runs-service 503"));
    const summary = await restoreCutFollowups({ dryRun: false, caller: CALLER });
    expect(summary).toMatchObject({ restored: 1, failed: 1 });
  });

  it("withoutTornDownOrgs is pure and keeps everything campaign-service did not stop for a teardown", () => {
    const kept = withoutTornDownOrgs(
      [{ campaignId: "a" }, { campaignId: "b" }, { campaignId: "c" }],
      new Map<string, string | null>([["a", "org_teardown"], ["b", "no_payment_method"]]),
    );
    expect(kept.map((k) => k.campaignId)).toEqual(["b", "c"]);
  });
});

describe("a retracted bounce gives the lead their follow-ups back", () => {
  it("after the retraction, the sequence's cut follow-ups are re-queued (Olive: 22 leads left on step 1)", async () => {
    // archive select, archive insert (mocked away), delete, delivery_status update,
    // org lookup, then the restore's candidate read.
    mockExecute.mockImplementation(async (query: unknown) => {
      const text = JSON.stringify((query as { queryChunks?: unknown[] }).queryChunks ?? query);
      if (text.includes("to_jsonb(e.*)")) return { rows: [{ event: { id: "ev-1" }, delivery_status: "bounced" }] };
      if (text.includes("SELECT instantly_campaign_id, org_id")) return { rows: [{ instantly_campaign_id: "self:r", org_id: OLIVE }] };
      if (text.includes("instantly_events_retracted r")) return { rows: [row("r", { paused: false })] };
      return { rows: [] };
    });

    await retractBouncedEvents(
      [{ id: "ev-1", campaignId: "self:r", leadEmail: "r@flowdesk.co" }],
      "message_refused_not_a_bounce",
      "retract:dsn-message-refused",
    );

    expect(mockRestoreOne).toHaveBeenCalledTimes(1);
    expect(mockRestoreOne.mock.calls[0]![0]).toMatchObject({ instantlyCampaignId: "self:r", steps: [2, 3] });
  });

  it("a failed re-queue never undoes the retraction (logged loud, the fleet restore picks it up)", async () => {
    mockExecute.mockImplementation(async (query: unknown) => {
      const text = JSON.stringify((query as { queryChunks?: unknown[] }).queryChunks ?? query);
      if (text.includes("instantly_events_retracted r")) throw new Error("db down");
      return { rows: [] };
    });
    await expect(
      retractBouncedEvents(
        [{ id: "ev-1", campaignId: "self:r", leadEmail: "r@flowdesk.co" }],
        "message_refused_not_a_bounce",
        "retract:dsn-message-refused",
      ),
    ).resolves.toBeUndefined();
  });
});
