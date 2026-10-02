import { describe, it, expect, vi, beforeEach } from "vitest";

// ── IO mocks ────────────────────────────────────────────────────────────────
const mockExecute = vi.fn();
const mockTxExecute = vi.fn();
const mockListLeadsFull = vi.fn();
const mockUpdateCampaignStatus = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockResolvePlatformKey = vi.fn();
const mockFindStandingOptOut = vi.fn();
const mockFindRecentBrandContact = vi.fn();
const mockCancelRemainingProvisions = vi.fn();
const mockRefreshLeadStatusCurrent = vi.fn();
const mockAnnounce = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
    transaction: async (fn: (tx: unknown) => Promise<unknown>) =>
      fn({ execute: (...args: unknown[]) => mockTxExecute(...args) }),
  },
}));
vi.mock("../../src/lib/instantly-client", () => ({
  listLeadsFull: (...args: unknown[]) => mockListLeadsFull(...args),
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));
vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
  resolvePlatformInstantlyApiKey: (...args: unknown[]) => mockResolvePlatformKey(...args),
}));
vi.mock("../../src/lib/lead-optouts", () => ({
  findStandingOptOut: (...args: unknown[]) => mockFindStandingOptOut(...args),
}));
vi.mock("../../src/lib/recontact-window", () => ({
  findRecentBrandContact: (...args: unknown[]) => mockFindRecentBrandContact(...args),
}));
vi.mock("../../src/lib/silver-promote", () => ({
  cancelRemainingProvisions: (...args: unknown[]) => mockCancelRemainingProvisions(...args),
}));
vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...args: unknown[]) => mockRefreshLeadStatusCurrent(...args),
}));
vi.mock("../../src/lib/evidence-changed", () => ({
  announceEvidenceChanged: (...args: unknown[]) => mockAnnounce(...args),
}));

const {
  sendingDaysElapsed,
  isStalledFirstEmail,
  decideStalledFirstEmail,
  instantlyShowsContact,
  sweepStalledFirstEmails,
  STALLED_FIRST_EMAIL_MAX_AGE_DAYS,
} = await import("../../src/lib/self-send/stalled-first-emails");

// Friday 2026-10-02, 09:00 UTC — the day #969 was measured.
const AS_OF = new Date("2026-10-02T09:00:00Z");

function candidate(over: Record<string, unknown> = {}) {
  return {
    instantlyCampaignId: "9a52588b-c6b8-4d17-abbf-a53f52d4e462",
    campaignId: "camp-1",
    orgId: "org-1",
    userId: "user-1",
    leadEmail: "jane@acme.com",
    brandIds: ["brand-1"],
    createdAt: new Date("2026-09-21T01:00:00Z"),
    hasFirstStepBody: true,
    hasQueuedStep: true,
    leadAnswered: false,
    newerSequence: false,
    optedOut: false,
    recentBrandContact: false,
    ...over,
  };
}

/** The candidate row as the loader's SQL returns it. */
function dbRow(over: Record<string, unknown> = {}) {
  const c = candidate(over);
  return { ...c, createdAt: c.createdAt.toISOString() };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockResolveInstantlyApiKey.mockResolvedValue({ key: "k" });
  mockResolvePlatformKey.mockResolvedValue("pk");
  mockFindStandingOptOut.mockResolvedValue(null);
  mockFindRecentBrandContact.mockResolvedValue(null);
  mockListLeadsFull.mockResolvedValue([{ id: "l", email: "jane@acme.com", status: 1, email_open_count: 0 }]);
  mockUpdateCampaignStatus.mockResolvedValue({});
  mockTxExecute.mockResolvedValue({ rows: [{ id: "row-1" }] });
});

describe("when a never-started first email counts as stalled", () => {
  it("counts only full sending days between the assignment day and today", () => {
    // Assigned Monday 09-28; Tue, Wed, Thu elapsed before Friday 10-02.
    expect(sendingDaysElapsed(new Date("2026-09-28T23:00:00Z"), AS_OF)).toBe(3);
    // Assigned Friday 09-25; the weekend does not count.
    expect(sendingDaysElapsed(new Date("2026-09-25T10:00:00Z"), AS_OF)).toBe(4);
    // Assigned yesterday.
    expect(sendingDaysElapsed(new Date("2026-10-01T10:00:00Z"), AS_OF)).toBe(0);
  });

  it("gives Instantly three sending days, not more and not fewer", () => {
    expect(isStalledFirstEmail(new Date("2026-09-28T01:00:00Z"), AS_OF)).toBe(true);
    expect(isStalledFirstEmail(new Date("2026-09-29T01:00:00Z"), AS_OF)).toBe(false);
  });
});

describe("move or close", () => {
  it("moves a sequence nothing argues against onto our own sender", () => {
    expect(decideStalledFirstEmail(candidate(), AS_OF)).toEqual({ action: "move" });
  });

  it.each([
    ["opted_out", { optedOut: true }],
    ["lead_answered", { leadAnswered: true }],
    ["newer_sequence", { newerSequence: true }],
    ["recent_brand_contact", { recentBrandContact: true }],
    ["no_step_body", { hasFirstStepBody: false }],
    ["no_queued_step", { hasQueuedStep: false }],
  ])("closes it when %s", (reason, over) => {
    expect(decideStalledFirstEmail(candidate(over), AS_OF)).toEqual({ action: "close", reason });
  });

  it("closes a first email assigned too long ago to send cold now", () => {
    const old = new Date(AS_OF.getTime() - (STALLED_FIRST_EMAIL_MAX_AGE_DAYS + 1) * 86_400_000);
    expect(decideStalledFirstEmail(candidate({ createdAt: old }), AS_OF)).toEqual({
      action: "close",
      reason: "too_old",
    });
  });

  it("puts a person's opt-out ahead of every other reading", () => {
    expect(
      decideStalledFirstEmail(candidate({ optedOut: true, hasFirstStepBody: false }), AS_OF),
    ).toEqual({ action: "close", reason: "opted_out" });
  });
});

describe("Instantly's own lead record", () => {
  it("an untouched lead shows no contact", () => {
    expect(instantlyShowsContact([{ id: "l", email: "a@b.c", status: 1, email_open_count: 0 }])).toBe(false);
  });
  it("a contact timestamp or any engagement means the sequence DID start", () => {
    expect(instantlyShowsContact([{ id: "l", email: "a@b.c", timestamp_last_contact: "2026-09-22T10:00:00Z" }])).toBe(true);
    expect(instantlyShowsContact([{ id: "l", email: "a@b.c", email_open_count: 1 }])).toBe(true);
  });
});

describe("sweepStalledFirstEmails", () => {
  it("dry run decides and counts, touching neither Instantly nor the DB", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [dbRow(), dbRow({ instantlyCampaignId: "c2", hasFirstStepBody: false }), dbRow({ instantlyCampaignId: "c3", createdAt: new Date("2026-10-01T01:00:00Z") })],
    });
    const summary = await sweepStalledFirstEmails({ asOf: AS_OF, dryRun: true });
    expect(summary).toMatchObject({ candidates: 3, stalled: 2, moved: 1, closed: 1, closedByReason: { no_step_body: 1 } });
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(mockListLeadsFull).not.toHaveBeenCalled();
    expect(mockTxExecute).not.toHaveBeenCalled();
    expect(mockExecute).toHaveBeenCalledTimes(1);
  });

  it("pauses the Instantly campaign BEFORE re-keying the sequence onto a self: id", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [dbRow()] });
    const order: string[] = [];
    mockUpdateCampaignStatus.mockImplementation(async () => { order.push("pause"); return {}; });
    mockTxExecute.mockImplementation(async () => { order.push("rekey"); return { rows: [{ id: "row-1" }] }; });

    const summary = await sweepStalledFirstEmails({ asOf: AS_OF });

    expect(summary.moved).toBe(1);
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith("k", "9a52588b-c6b8-4d17-abbf-a53f52d4e462", "paused");
    expect(order[0]).toBe("pause");
    expect(order.filter((o) => o === "rekey")).toHaveLength(5);
    // The campaign row flips to smtp under a fresh self: id; holds follow it.
    const sqlText = (call: unknown[]) =>
      JSON.stringify((call[0] as { queryChunks?: unknown[] }).queryChunks ?? call[0]);
    const first = sqlText(mockTxExecute.mock.calls[0]!);
    expect(first).toContain("UPDATE instantly_campaigns");
    expect(first).toMatch(/self:[0-9a-f-]{36}/);
    expect(sqlText(mockTxExecute.mock.calls[1]!)).toContain("UPDATE sequence_costs");
    expect(mockCancelRemainingProvisions).not.toHaveBeenCalled();
  });

  it("closes: refunds the holds, then marks the row, then announces", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [dbRow({ hasFirstStepBody: false })] });
    mockExecute.mockResolvedValue({ rows: [] });

    const summary = await sweepStalledFirstEmails({ asOf: AS_OF });

    expect(summary).toMatchObject({ closed: 1, moved: 0, closedByReason: { no_step_body: 1 } });
    expect(mockUpdateCampaignStatus).toHaveBeenCalledTimes(1);
    expect(mockCancelRemainingProvisions).toHaveBeenCalledTimes(1);
    expect(mockRefreshLeadStatusCurrent).toHaveBeenCalledWith("9a52588b-c6b8-4d17-abbf-a53f52d4e462", "jane@acme.com");
    expect(mockAnnounce).toHaveBeenCalledWith("org-1", ["jane@acme.com"], "stalled_first_email_closed");
    expect(mockTxExecute).not.toHaveBeenCalled();
  });

  it("leaves a row untouched when Instantly says the sequence did start", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [dbRow()] });
    mockListLeadsFull.mockResolvedValue([{ id: "l", email: "jane@acme.com", timestamp_last_contact: "2026-09-22T10:00:00Z" }]);

    const summary = await sweepStalledFirstEmails({ asOf: AS_OF });

    expect(summary).toMatchObject({ skippedContactedOnInstantly: 1, moved: 0, closed: 0 });
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(mockTxExecute).not.toHaveBeenCalled();
  });

  it("does nothing to a row whose Instantly pause fails, so Instantly can never double up", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [dbRow()] });
    mockUpdateCampaignStatus.mockRejectedValue(new Error("instantly-api POST /campaigns/x/pause failed: 500 - boom"));

    const summary = await sweepStalledFirstEmails({ asOf: AS_OF });

    expect(summary).toMatchObject({ failed: 1, moved: 0, closed: 0 });
    expect(mockTxExecute).not.toHaveBeenCalled();
    expect(mockCancelRemainingProvisions).not.toHaveBeenCalled();
  });

  it("treats a campaign Instantly no longer has as unable to send, and proceeds", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [dbRow()] });
    mockListLeadsFull.mockRejectedValue(new Error("instantly-api POST /leads/list failed: 404 - not found"));
    mockUpdateCampaignStatus.mockRejectedValue(new Error("instantly-api POST /campaigns/x/pause failed: 404 - not found"));

    const summary = await sweepStalledFirstEmails({ asOf: AS_OF });

    expect(summary.moved).toBe(1);
  });

  it("respects the per-sweep limit", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [dbRow(), dbRow({ instantlyCampaignId: "c2" }), dbRow({ instantlyCampaignId: "c3" })] });
    const summary = await sweepStalledFirstEmails({ asOf: AS_OF, limit: 2 });
    expect(summary).toMatchObject({ stalled: 3, moved: 2 });
  });
});
