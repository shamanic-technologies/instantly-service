import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockAlert = vi.fn();

vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));
vi.mock("../../src/lib/alert-unanswered-info-request", () => ({
  INFO_REQUEST_KIND: "lead_info_requested",
  maybeAlertUnansweredInfoRequest: (...a: unknown[]) => mockAlert(...a),
}));

import {
  alertOverdueInfoRequests,
  INFO_REQUEST_ANSWER_DEADLINE_MS,
  sinceLabel,
} from "../../src/lib/info-request-deadline";

const row = {
  instantly_campaign_id: "ic-1",
  lead_email: "drgallego@shinwellness.com",
  campaign_id: "c-1",
  org_id: "org-1",
  user_id: "u-1",
  run_id: "r-1",
  brand_ids: ["b-1"],
  known_at: "2026-10-08T08:36:41.955Z",
};

describe("unanswered question deadline sweep", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockAlert.mockResolvedValue({ sent: true, why: "x" });
  });

  it("deadline is a working day (past the responder's 5 h median answer)", () => {
    expect(INFO_REQUEST_ANSWER_DEADLINE_MS).toBe(24 * 60 * 60 * 1000);
  });

  it("alerts every overdue thread in OVERDUE mode (responder status not asked)", async () => {
    mockExecute.mockResolvedValue({ rows: [row] });
    const out = await alertOverdueInfoRequests(new Date("2026-10-11T12:00:00Z"));
    expect(out).toEqual({ overdue: 1, alerted: 1 });
    const [campaign, lead, kind, options] = mockAlert.mock.calls[0];
    expect(campaign).toMatchObject({ instantlyCampaignId: "ic-1", orgId: "org-1", runId: "r-1", brandIds: ["b-1"] });
    expect(lead).toBe("drgallego@shinwellness.com");
    expect(kind).toBe("lead_info_requested");
    expect(options).toEqual({ overdueSince: "on Oct 8" });
  });

  it("the read keys on the outcome: no answer sent since we learned of the question, not alerted yet", async () => {
    mockExecute.mockResolvedValue({ rows: [] });
    await alertOverdueInfoRequests(new Date("2026-10-11T12:00:00Z"));
    const query = JSON.stringify(mockExecute.mock.calls[0][0]);
    expect(query).toContain("smtp_dispatch_raw");
    expect(query).toContain("d.step = 0");
    expect(query).toContain("ue_type' = '3'");
    expect(query).toContain("infoRequestUnansweredAlert");
    expect(query).toContain("g.reply_kind");
    expect(mockAlert).not.toHaveBeenCalled();
  });

  it("a failed read throws (the worker logs it loud), nothing is alerted", async () => {
    mockExecute.mockRejectedValue(new Error("pool timeout"));
    await expect(alertOverdueInfoRequests()).rejects.toThrow("pool timeout");
    expect(mockAlert).not.toHaveBeenCalled();
  });

  it("concurrent calls join one sweep", async () => {
    mockExecute.mockResolvedValue({ rows: [row] });
    const [a, b] = await Promise.all([alertOverdueInfoRequests(), alertOverdueInfoRequests()]);
    expect(a).toBe(b);
    expect(mockExecute).toHaveBeenCalledTimes(1);
  });

  it("sinceLabel reads like a date", () => {
    expect(sinceLabel(new Date("2026-10-05T19:40:17Z"))).toBe("on Oct 5");
  });
});
