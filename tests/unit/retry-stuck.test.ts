import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────
//
// The simplified retry-stuck module exposes two primitives consumed by the
// worker loop:
//   - selectOneStuckRow(): SELECT ... LIMIT 1 returning a candidate or null.
//   - processRow(row):     resolve key, recover sequence, send on fresh
//                          account, refund + recharge, mute row. Returns a
//                          discriminated RowOutcome; never throws.
//
// No advisory lock. No batching. No counters. The worker is responsible for
// looping; this module is stateless side-effects.

const mockDbExecute = vi.fn();
const mockDbUpdateSet = vi.fn();
const mockDbInsertValues = vi.fn();
const mockDbSelectQueue: unknown[][] = [];

function nextDbSelectResponse(): unknown[] {
  return mockDbSelectQueue.shift() ?? [];
}

function makeSelectChain() {
  return {
    from: () => ({
      where: () => {
        const rows = nextDbSelectResponse();
        const thenable = Promise.resolve(rows) as Promise<unknown[]> & {
          limit: (n: number) => Promise<unknown[]>;
        };
        thenable.limit = () => Promise.resolve(rows);
        return thenable;
      },
    }),
  };
}

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockDbExecute(...args),
    select: () => makeSelectChain(),
    update: () => ({
      set: (v: unknown) => ({
        where: () => {
          mockDbUpdateSet(v);
          return Promise.resolve([{}]);
        },
      }),
    }),
    insert: () => ({
      values: (v: unknown) => {
        mockDbInsertValues(v);
        return {
          onConflictDoNothing: () => Promise.resolve([{}]),
        };
      },
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { id: "id", instantlyCampaignId: "instantly_campaign_id" },
  instantlyLeads: { instantlyCampaignId: "instantly_campaign_id", email: "email" },
  sequenceCosts: { campaignId: "campaign_id", leadEmail: "lead_email", status: "status" },
}));

const mockResolveKey = vi.fn();

const { MockKeyServiceError } = vi.hoisted(() => {
  class MockKeyServiceError extends Error {
    statusCode: number;
    constructor(statusCode: number, message: string) {
      super(message);
      this.name = "KeyServiceError";
      this.statusCode = statusCode;
    }
  }
  return { MockKeyServiceError };
});

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveKey(...args),
  KeyServiceError: MockKeyServiceError,
}));

const mockGetCampaign = vi.fn();

vi.mock("../../src/lib/instantly-client", () => ({
  getCampaign: (...args: unknown[]) => mockGetCampaign(...args),
}));

const mockSendLeadToInstantly = vi.fn();
const mockStripAccountSignature = vi.fn((body: string) => body);

vi.mock("../../src/lib/send-lead", () => ({
  sendLeadToInstantly: (...args: unknown[]) => mockSendLeadToInstantly(...args),
  stripAccountSignature: (b: string) => mockStripAccountSignature(b),
}));

const mockCreateRun = vi.fn();
const mockUpdateRun = vi.fn();
const mockAddCosts = vi.fn();
const mockUpdateCostStatus = vi.fn();

vi.mock("../../src/lib/runs-client", () => ({
  createRun: (...args: unknown[]) => mockCreateRun(...args),
  updateRun: (...args: unknown[]) => mockUpdateRun(...args),
  addCosts: (...args: unknown[]) => mockAddCosts(...args),
  updateCostStatus: (...args: unknown[]) => mockUpdateCostStatus(...args),
}));

import {
  selectOneStuckRow,
  processRow,
  STUCK_AGE_HOURS,
  type StuckCampaignRow,
} from "../../src/lib/retry-stuck";

function row(extra: Record<string, unknown> = {}): StuckCampaignRow {
  return {
    id: "row-1",
    instantlyCampaignId: "inst-camp-1",
    campaignId: "camp-1",
    leadEmail: "lead@test.com",
    orgId: "org-1",
    userId: "user-1",
    runId: "run-1",
    brandIds: ["brand-1"],
    metadata: null,
    ...extra,
  };
}

const SINGLE_STEP_LIVE = {
  sequences: [
    {
      steps: [
        { delay: 0, variants: [{ subject: "Hi", body: "Body\n\n--\nold-sig" }] },
      ],
    },
  ],
};

function queueSelectLead() {
  mockDbSelectQueue.push([
    {
      email: "lead@test.com",
      firstName: "Lead",
      lastName: "Doe",
      companyName: "Co",
      customVariables: null,
    },
  ]);
}

function queueSelectCosts(costs: unknown[] = []) {
  mockDbSelectQueue.push(costs);
}

beforeEach(() => {
  vi.resetAllMocks();
  mockDbSelectQueue.length = 0;
  mockDbExecute.mockResolvedValue({ rows: [] });
  mockResolveKey.mockResolvedValue({ key: "fake-api-key", keySource: "platform" });
  mockGetCampaign.mockResolvedValue(SINGLE_STEP_LIVE);
  mockSendLeadToInstantly.mockResolvedValue({
    ok: true,
    value: {
      instantlyCampaignId: "inst-camp-NEW",
      added: 1,
      account: { email: "new-sender@test.com", status: 1, warmup_status: 1 },
    },
  });
  mockStripAccountSignature.mockImplementation((b: string) => b);
  mockCreateRun.mockImplementation(async () => ({
    id: `step-run-${Math.random().toString(36).slice(2, 8)}`,
  }));
  mockUpdateRun.mockResolvedValue({});
  mockAddCosts.mockResolvedValue({
    costs: [
      { id: "new-cost-account-id", costName: "instantly-account-email-sent" },
      { id: "new-cost-domain-id", costName: "instantly-domain-email-sent" },
    ],
  });
  mockUpdateCostStatus.mockResolvedValue({});
});

// ─── Constants ─────────────────────────────────────────────────────────────

describe("STUCK_AGE_HOURS", () => {
  it("is 72", () => {
    expect(STUCK_AGE_HOURS).toBe(72);
  });
});

// ─── selectOneStuckRow ─────────────────────────────────────────────────────

describe("selectOneStuckRow", () => {
  it("SQL contains 72h floor, LIMIT 1, ORDER BY ASC, silver NOT EXISTS guard, and NULL identifier filter", async () => {
    await selectOneStuckRow();
    const selectCall = mockDbExecute.mock.calls[0]?.[0];
    expect(selectCall).toBeDefined();
    const text = chunkText(selectCall);
    expect(text).toMatch(/72\s+hours/);
    expect(text).toMatch(/ORDER BY created_at ASC/);
    expect(text).toMatch(/LIMIT 1/);
    expect(text).toMatch(/NOT EXISTS/i);
    expect(text).toMatch(/instantly_events/);
    expect(text).toMatch(/email_sent/);
    expect(text).toMatch(/email_bounced/);
    expect(text).toMatch(/reply_received/);
    expect(text).toMatch(/lead_unsubscribed/);
    expect(text).toMatch(/c\.campaign_id IS NOT NULL/);
    expect(text).toMatch(/c\.lead_email IS NOT NULL/);
    expect(text).toMatch(/c\.org_id IS NOT NULL/);
  });

  it("returns null when SELECT yields zero rows", async () => {
    mockDbExecute.mockResolvedValueOnce({ rows: [] });
    const r = await selectOneStuckRow();
    expect(r).toBeNull();
  });

  it("returns the candidate row when SELECT yields one", async () => {
    mockDbExecute.mockResolvedValueOnce({ rows: [row()] });
    const r = await selectOneStuckRow();
    expect(r).not.toBeNull();
    expect(r!.id).toBe("row-1");
    expect(r!.instantlyCampaignId).toBe("inst-camp-1");
  });
});

// ─── processRow success ────────────────────────────────────────────────────

describe("processRow — success path", () => {
  it("re-sends the lead on a fresh healthy account", async () => {
    queueSelectLead();
    queueSelectCosts([
      {
        id: "cost-row-1",
        runId: "step-run-1",
        costId: "cost-id-1",
        status: "provisioned",
        step: 1,
        campaignId: "camp-1",
        leadEmail: "lead@test.com",
      },
    ]);

    const outcome = await processRow(row());

    expect(outcome).toEqual({
      kind: "redispatched",
      newInstantlyCampaignId: "inst-camp-NEW",
      account: "new-sender@test.com",
    });

    // sendLeadToInstantly invoked with rebuilt sequence + lead.
    expect(mockSendLeadToInstantly).toHaveBeenCalledTimes(1);
    const args = mockSendLeadToInstantly.mock.calls[0][0] as {
      sortedSequence: Array<{ step: number; bodyHtml: string; daysSinceLastStep: number }>;
      lead: { email: string; first_name?: string };
      subject: string;
    };
    expect(args.lead.email).toBe("lead@test.com");
    expect(args.lead.first_name).toBe("Lead");
    expect(args.sortedSequence).toHaveLength(1);
    expect(args.subject).toBe("Hi");

    // Old cost cancelled via runs-service.
    expect(mockUpdateCostStatus).toHaveBeenCalledWith(
      "step-run-1",
      "cost-id-1",
      "cancelled",
      expect.objectContaining({ orgId: "org-1" }),
    );
    // Fresh cost provisioned for the new step run.
    expect(mockAddCosts).toHaveBeenCalled();

    // Row mutated to point at the new Instantly campaign.
    const muteCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.instantlyCampaignId === "inst-camp-NEW";
    });
    expect(muteCall).toBeDefined();
  });

  it("appends redispatchHistory + bumps redispatchCount on success", async () => {
    queueSelectLead();
    queueSelectCosts([]);

    await processRow(
      row({
        metadata: {
          redispatchCount: 1,
          redispatchHistory: [
            { from: "inst-OLD", to: "inst-camp-1", account: "a@test.com", at: "2026-01-01T00:00:00.000Z" },
          ],
        },
      }),
    );

    const muteCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return "instantlyCampaignId" in v;
    });
    expect(muteCall).toBeDefined();
    const v = muteCall![0] as { metadata: { redispatchCount: number; redispatchHistory: unknown[] } };
    expect(v.metadata.redispatchCount).toBe(2);
    expect(v.metadata.redispatchHistory).toHaveLength(2);
  });

  it("mirrors the lead onto the new Instantly campaign", async () => {
    queueSelectLead();
    queueSelectCosts([]);

    await processRow(row());

    const leadInsert = mockDbInsertValues.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.instantlyCampaignId === "inst-camp-NEW" && v.email === "lead@test.com";
    });
    expect(leadInsert).toBeDefined();
  });
});

// ─── processRow failure paths — row left alone ─────────────────────────────

describe("processRow — failure paths", () => {
  it("returns skipped_no_key when KeyServiceError 404", async () => {
    mockResolveKey.mockRejectedValueOnce(new MockKeyServiceError(404, "key not configured"));

    const outcome = await processRow(row());

    expect(outcome).toEqual({ kind: "skipped_no_key" });
    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
  });

  it("returns failed when sendLeadToInstantly returns no_healthy_account", async () => {
    queueSelectLead();
    mockSendLeadToInstantly.mockResolvedValueOnce({ ok: false, reason: "no_healthy_account" });

    const outcome = await processRow(row());

    expect(outcome).toEqual({ kind: "failed", reason: "no_healthy_account" });
    const muteCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.instantlyCampaignId === "inst-camp-NEW";
    });
    expect(muteCall).toBeUndefined();
  });

  it("returns failed when sendLeadToInstantly returns max_retries_exhausted", async () => {
    queueSelectLead();
    mockSendLeadToInstantly.mockResolvedValueOnce({ ok: false, reason: "max_retries_exhausted" });

    const outcome = await processRow(row());

    expect(outcome).toEqual({ kind: "failed", reason: "max_retries_exhausted" });
  });

  it("returns failed when live campaign has no sequence", async () => {
    mockGetCampaign.mockResolvedValueOnce({ sequences: [] });

    const outcome = await processRow(row());

    expect(outcome).toEqual({ kind: "failed", reason: "no_sequence" });
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
  });

  it("returns failed when local lead profile is missing", async () => {
    // No queueSelectLead() — lookup returns empty array.

    const outcome = await processRow(row());

    expect(outcome).toEqual({ kind: "failed", reason: "lead_profile_not_found" });
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
  });

  it("returns failed (with thrown message) when getCampaign throws", async () => {
    mockGetCampaign.mockRejectedValueOnce(new Error("instantly 500"));

    const outcome = await processRow(row());

    expect(outcome.kind).toBe("failed");
    if (outcome.kind === "failed") {
      expect(outcome.reason).toContain("instantly 500");
    }
  });

  it("returns failed when row is missing required identifiers", async () => {
    const outcome = await processRow(row({ campaignId: null }));

    expect(outcome).toEqual({ kind: "failed", reason: "missing_identifiers" });
    expect(mockResolveKey).not.toHaveBeenCalled();
  });
});

/** Recursively concatenate every string fragment in a drizzle SQL query. */
function chunkText(query: unknown): string {
  if (query == null) return "";
  if (typeof query === "string") return query;
  if (typeof query !== "object") return String(query);

  const chunks = (query as { queryChunks?: unknown[] }).queryChunks;
  if (Array.isArray(chunks)) {
    return chunks.map(chunkText).join("");
  }

  const v = (query as { value?: unknown }).value;
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return v.map(chunkText).join("");

  return "";
}
