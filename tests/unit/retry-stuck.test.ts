import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────
//
// New simplified retry-stuck flow per row:
//   1. getCampaign(apiKey, instantlyCampaignId)         → live (sequence read)
//   2. extractSequenceFromLive(live)                    → seq
//   3. SELECT instantly_leads .. WHERE instantly_campaign_id = ... LIMIT 1
//   4. sendLeadToInstantly({...})                       → fresh campaign on healthy account
//   5. SELECT sequence_costs .. WHERE campaign_id = ... AND lead_email = ...
//   6. updateCostStatus for each → cancel old (refund)
//   7. createRun + addCosts per step + updateRun        → fresh provisioned costs (recharge)
//   8. db.insert(instantlyLeads) onConflictDoNothing    → mirror lead onto new campaign
//   9. db.update(instantlyCampaigns)                    → mute row (point at new campaign)
//
// NO NSS read. NO pause call. NO terminal cancel (handleCampaignError). NO max
// retry cap. On any failure (no sequence, no lead, send failure, getCampaign
// throw) the row is left alone — next tick retries.

const mockDbExecute = vi.fn();
const mockDbUpdateSet = vi.fn();
const mockDbInsertValues = vi.fn();
const mockDbSelectQueue: unknown[][] = [];

function nextDbSelectResponse(): unknown[] {
  return mockDbSelectQueue.shift() ?? [];
}

/**
 * Drizzle select chain in retry-stuck:
 *   - `db.select().from(...).where(...)`            sequenceCosts lookup
 *   - `db.select().from(...).where(...).limit(n)`   instantly_leads lookup
 * Each `db.select().from().where()` consumes ONE queue item; `.limit()` no-ops.
 */
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
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/lib/instantly-client", () => ({
  getCampaign: (...args: unknown[]) => mockGetCampaign(...args),
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

// retry-stuck no longer calls campaign-error-handler. Spy that it stays unused.
const mockHandleCampaignError = vi.fn();

vi.mock("../../src/lib/campaign-error-handler", () => ({
  handleCampaignError: (...args: unknown[]) => mockHandleCampaignError(...args),
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
  runRetryStuck,
  MAX_ROWS_PER_TICK,
  STUCK_AGE_HOURS,
  BATCH_SIZE,
} from "../../src/lib/retry-stuck";

function row(extra: Record<string, unknown> = {}) {
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

function setupSweep(rows: Array<Record<string, unknown>>, opts: { lockAcquired?: boolean } = {}) {
  const lockAcquired = opts.lockAcquired ?? true;
  mockDbExecute.mockResolvedValueOnce({ rows: [{ locked: lockAcquired }] });
  if (lockAcquired) {
    mockDbExecute.mockResolvedValueOnce({ rows });
  }
}

describe("runRetryStuck", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockDbSelectQueue.length = 0;
    mockDbExecute.mockResolvedValue({ rows: [] });
    mockResolveKey.mockResolvedValue({ key: "fake-api-key", keySource: "platform" });
    mockGetCampaign.mockResolvedValue(SINGLE_STEP_LIVE);
    mockUpdateCampaignStatus.mockResolvedValue({});
    mockHandleCampaignError.mockResolvedValue(undefined);
    mockSendLeadToInstantly.mockResolvedValue({
      ok: true,
      value: {
        instantlyCampaignId: "inst-camp-NEW",
        added: 1,
        account: { email: "new-sender@test.com", status: 1, warmup_status: 1 },
      },
    });
    mockStripAccountSignature.mockImplementation((b: string) => b);
    mockCreateRun.mockImplementation(async () => ({ id: `step-run-${Math.random().toString(36).slice(2, 8)}` }));
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

  it("STUCK_AGE_HOURS is 72", () => {
    expect(STUCK_AGE_HOURS).toBe(72);
  });

  it("MAX_ROWS_PER_TICK is bounded (≤ 200) so a tick stays cheap", () => {
    expect(MAX_ROWS_PER_TICK).toBeGreaterThan(0);
    expect(MAX_ROWS_PER_TICK).toBeLessThanOrEqual(200);
  });

  it("BATCH_SIZE > 1 so per-tick parallelism is on", () => {
    expect(BATCH_SIZE).toBeGreaterThan(1);
  });

  // ─── SQL selection filter ──────────────────────────────────────────────────

  it("selectStuckRows SQL contains 72h floor, LIMIT, ORDER BY ASC, silver NOT EXISTS guard, and NULL identifier filter", async () => {
    setupSweep([]);
    await runRetryStuck();
    const selectCall = mockDbExecute.mock.calls[1]?.[0];
    expect(selectCall).toBeDefined();
    const text = chunkText(selectCall);
    expect(text).toMatch(/72\s+hours/);
    expect(text).toMatch(/ORDER BY created_at ASC/);
    expect(text).toMatch(/LIMIT/);
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

  // ─── Lock + grouping ───────────────────────────────────────────────────────

  it("short-circuits with skipped='sweep_in_progress' when advisory lock is held", async () => {
    setupSweep([], { lockAcquired: false });

    const summary = await runRetryStuck();

    expect(summary.skipped).toBe("sweep_in_progress");
    expect(summary.scanned).toBe(0);
    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockResolveKey).not.toHaveBeenCalled();
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
  });

  it("groups rows by orgId so each org resolves its key once", async () => {
    setupSweep([
      row({ id: "a", orgId: "org-1" }),
      row({ id: "b", orgId: "org-1", instantlyCampaignId: "inst-b" }),
      row({ id: "c", orgId: "org-2", instantlyCampaignId: "inst-c" }),
    ]);
    queueSelectLead();
    queueSelectCosts([]);
    queueSelectLead();
    queueSelectCosts([]);
    queueSelectLead();
    queueSelectCosts([]);

    await runRetryStuck();

    expect(mockResolveKey).toHaveBeenCalledTimes(2);
  });

  it("skips org when key resolution fails", async () => {
    setupSweep([row()]);
    mockResolveKey.mockRejectedValueOnce(new MockKeyServiceError(404, "key not configured"));

    const summary = await runRetryStuck();

    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
    expect(summary.skippedNoKey).toBe(1);
  });

  it("caps work at MAX_ROWS_PER_TICK", async () => {
    const rows = Array.from({ length: MAX_ROWS_PER_TICK }, (_, i) =>
      row({
        id: `row-${i}`,
        instantlyCampaignId: `inst-${i}`,
        leadEmail: `lead-${i}@test.com`,
      }),
    );
    setupSweep(rows);
    for (let i = 0; i < MAX_ROWS_PER_TICK; i++) {
      queueSelectLead();
      queueSelectCosts([]);
    }

    const summary = await runRetryStuck();

    expect(summary.scanned).toBe(MAX_ROWS_PER_TICK);
  });

  // ─── Success flow ──────────────────────────────────────────────────────────

  it("re-dispatches on a fresh healthy account", async () => {
    setupSweep([row()]);
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

    const summary = await runRetryStuck();

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

    expect(summary.redispatched).toBe(1);
    expect(summary.failed).toBe(0);
    expect(summary.scanned).toBe(1);
  });

  it("appends redispatchHistory + bumps redispatchCount on successful re-dispatch", async () => {
    setupSweep([
      row({
        metadata: {
          redispatchCount: 1,
          redispatchHistory: [
            { from: "inst-OLD", to: "inst-camp-1", account: "a@test.com", at: "2026-01-01T00:00:00.000Z" },
          ],
        },
      }),
    ]);
    queueSelectLead();
    queueSelectCosts([]);

    await runRetryStuck();

    const muteCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return "instantlyCampaignId" in v;
    });
    expect(muteCall).toBeDefined();
    const v = muteCall![0] as { metadata: { redispatchCount: number; redispatchHistory: unknown[] } };
    expect(v.metadata.redispatchCount).toBe(2);
    expect(v.metadata.redispatchHistory).toHaveLength(2);
  });

  it("mirrors the lead onto the new Instantly campaign so future re-dispatches resolve", async () => {
    setupSweep([row()]);
    queueSelectLead();
    queueSelectCosts([]);

    await runRetryStuck();

    const leadInsert = mockDbInsertValues.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.instantlyCampaignId === "inst-camp-NEW" && v.email === "lead@test.com";
    });
    expect(leadInsert).toBeDefined();
  });

  // ─── Failure flow — row left alone, NOT terminal-cancelled ─────────────────

  it("when sendLeadToInstantly returns no_healthy_account, row is left alone (no terminal cancel)", async () => {
    setupSweep([row()]);
    queueSelectLead();
    mockSendLeadToInstantly.mockResolvedValueOnce({ ok: false, reason: "no_healthy_account" });

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    const muteCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.instantlyCampaignId === "inst-camp-NEW";
    });
    expect(muteCall).toBeUndefined();
    expect(summary.redispatched).toBe(0);
    expect(summary.failed).toBe(1);
  });

  it("when sendLeadToInstantly returns max_retries_exhausted, row is left alone (no terminal cancel)", async () => {
    setupSweep([row()]);
    queueSelectLead();
    mockSendLeadToInstantly.mockResolvedValueOnce({ ok: false, reason: "max_retries_exhausted" });

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(summary.redispatched).toBe(0);
    expect(summary.failed).toBe(1);
  });

  it("when live campaign has no sequence, row is left alone (no terminal cancel)", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({ sequences: [] });

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
    expect(summary.failed).toBe(1);
  });

  it("when local lead profile is missing, row is left alone (no terminal cancel)", async () => {
    setupSweep([row()]);
    // No queueSelectLead() — lookup returns empty array.

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(mockSendLeadToInstantly).not.toHaveBeenCalled();
    expect(summary.failed).toBe(1);
  });

  it("when getCampaign throws, row is counted as failed but the sweep continues", async () => {
    setupSweep([
      row({ id: "a", instantlyCampaignId: "inst-a" }),
      row({ id: "b", instantlyCampaignId: "inst-b" }),
    ]);
    mockGetCampaign
      .mockRejectedValueOnce(new Error("instantly 500"))
      .mockResolvedValueOnce(SINGLE_STEP_LIVE);
    queueSelectLead();
    queueSelectCosts([]);

    const summary = await runRetryStuck();

    expect(summary.scanned).toBe(2);
    expect(summary.failed).toBe(1);
    expect(summary.redispatched).toBe(1);
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
  });

  // ─── Removed-side-effect guards ────────────────────────────────────────────

  it("never calls updateCampaignStatus (no pause on the old campaign)", async () => {
    setupSweep([row()]);
    queueSelectLead();
    queueSelectCosts([]);

    await runRetryStuck();

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("never writes not_sending_status onto the row (reconcile owns NSS, not retry-stuck)", async () => {
    setupSweep([row()]);
    queueSelectLead();
    queueSelectCosts([]);

    await runRetryStuck();

    // Every db.update().set(v) — none of them should set notSendingStatus.
    for (const call of mockDbUpdateSet.mock.calls) {
      const v = call[0] as Record<string, unknown>;
      expect(v).not.toHaveProperty("notSendingStatus");
    }
  });
});

/** Recursively concatenate every string fragment in a drizzle SQL query. */
function chunkText(query: unknown): string {
  if (query == null) return "";
  if (typeof query === "string") return query;
  if (typeof query !== "object") return String(query);

  // Drizzle's SQL node nests recursively via `queryChunks`.
  const chunks = (query as { queryChunks?: unknown[] }).queryChunks;
  if (Array.isArray(chunks)) {
    return chunks.map(chunkText).join("");
  }

  // sql.raw and StringChunk both expose their text via `.value`.
  const v = (query as { value?: unknown }).value;
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return v.map(chunkText).join("");

  return "";
}
