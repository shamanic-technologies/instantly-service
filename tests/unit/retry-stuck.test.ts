import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────
//
// runRetryStuck calls many primitives. Mocks dispatch by drizzle table & method
// where it matters; the rest fall through to `vi.fn()` defaults.
//
// db.execute is called multiple times per sweep:
//   1. SELECT pg_try_advisory_lock(...) AS locked
//   2. SELECT ... FROM instantly_campaigns ... LIMIT 500
//   3. SELECT pg_advisory_unlock(...)
// Tests use `setupSweep(rows)` to queue (1) + (2). The catch-all default
// `{ rows: [] }` handles (3) and any UPDATE/SELECT/INSERT through db.execute.

const mockDbExecute = vi.fn();
const mockDbUpdateSet = vi.fn();
const mockDbInsertValues = vi.fn();
const mockDbSelectQueue: unknown[][] = [];

function nextDbSelectResponse(): unknown[] {
  return mockDbSelectQueue.shift() ?? [];
}

/**
 * The drizzle select chain has two shapes in retry-stuck:
 *   - `db.select().from(...).where(...)`            awaited directly (sequenceCosts lookup)
 *   - `db.select().from(...).where(...).limit(n)`   awaited (instantly_leads lookup)
 *
 * Build a thenable that resolves to the next queued response AND exposes a
 * `.limit()` method that returns another thenable with the same response.
 * Each call to `db.select().from(...).where(...)` consumes ONE item from the
 * queue, regardless of whether `.limit()` is then chained.
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
  instantlyCampaigns: {
    id: "id",
    instantlyCampaignId: "instantly_campaign_id",
    notSendingStatus: "not_sending_status",
    notSendingStatusSeenAt: "not_sending_status_seen_at",
  },
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

const mockHandleCampaignError = vi.fn();

vi.mock("../../src/lib/campaign-error-handler", () => ({
  handleCampaignError: (...args: unknown[]) => mockHandleCampaignError(...args),
}));

const mockDispatchLeadToInstantly = vi.fn();
const mockStripAccountSignature = vi.fn((body: string) => body);

vi.mock("../../src/lib/dispatch-lead", () => ({
  dispatchLeadToInstantly: (...args: unknown[]) => mockDispatchLeadToInstantly(...args),
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
  MAX_ROWS_PER_RUN,
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
  not_sending_status: { reason: "x" },
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
    mockGetCampaign.mockResolvedValue({});
    mockUpdateCampaignStatus.mockResolvedValue({});
    mockHandleCampaignError.mockResolvedValue(undefined);
    mockDispatchLeadToInstantly.mockResolvedValue({
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

  // ─── Still-sending / lock / cap (carried over from PR C tests) ─────────────

  it("skips when not_sending_status is null", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({ not_sending_status: null });

    const summary = await runRetryStuck();

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(mockDispatchLeadToInstantly).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(summary.stillSending).toBe(1);
    expect(summary.cancelled).toBe(0);
    expect(summary.redispatched).toBe(0);
  });

  it("skips when not_sending_status is missing (undefined)", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({});

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(mockDispatchLeadToInstantly).not.toHaveBeenCalled();
    expect(summary.stillSending).toBe(1);
  });

  it("short-circuits with skipped='sweep_in_progress' when the advisory lock is held", async () => {
    setupSweep([], { lockAcquired: false });

    const summary = await runRetryStuck();

    expect(summary.skipped).toBe("sweep_in_progress");
    expect(summary.scanned).toBe(0);
    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(mockResolveKey).not.toHaveBeenCalled();
  });

  it("limits work to MAX_ROWS_PER_RUN rows in a single sweep", async () => {
    const rows = Array.from({ length: MAX_ROWS_PER_RUN }, (_, i) =>
      row({
        id: `row-${i}`,
        instantlyCampaignId: `inst-${i}`,
        leadEmail: `lead-${i}@test.com`,
      }),
    );
    setupSweep(rows);
    mockGetCampaign.mockResolvedValue({ not_sending_status: null });

    const summary = await runRetryStuck();

    expect(summary.scanned).toBe(MAX_ROWS_PER_RUN);
  });

  it("selectStuckRows SQL contains LIMIT and ORDER BY created_at ASC", async () => {
    setupSweep([]);
    await runRetryStuck();
    const selectCall = mockDbExecute.mock.calls[1]?.[0];
    expect(selectCall).toBeDefined();
    const text = chunkText(selectCall);
    expect(text).toMatch(/ORDER BY created_at ASC/);
    expect(text).toMatch(/LIMIT/);
  });

  it("exposes BATCH_SIZE > 1 so per-tick parallelism is on", () => {
    expect(BATCH_SIZE).toBeGreaterThan(1);
  });

  it("skips org when key resolution fails", async () => {
    setupSweep([row()]);
    mockResolveKey.mockRejectedValueOnce(new MockKeyServiceError(404, "key not configured"));

    const summary = await runRetryStuck();

    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockDispatchLeadToInstantly).not.toHaveBeenCalled();
    expect(summary.skippedNoKey).toBe(1);
  });

  it("groups rows by orgId so each org resolves its key once", async () => {
    setupSweep([
      row({ id: "a", orgId: "org-1" }),
      row({ id: "b", orgId: "org-1" }),
      row({ id: "c", orgId: "org-2", instantlyCampaignId: "inst-c" }),
    ]);
    mockGetCampaign.mockResolvedValue({ not_sending_status: null });

    await runRetryStuck();

    expect(mockResolveKey).toHaveBeenCalledTimes(2);
  });

  // ─── New: re-dispatch flow ─────────────────────────────────────────────────

  it("on NSS, attempts re-dispatch onto a fresh healthy account", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce(SINGLE_STEP_LIVE);
    queueSelectLead();
    queueSelectCosts([
      { id: "cost-row-1", runId: "step-run-1", costId: "cost-id-1", status: "provisioned", step: 1, campaignId: "camp-1", leadEmail: "lead@test.com" },
    ]);

    const summary = await runRetryStuck();

    // dispatchLeadToInstantly invoked with the rebuilt sequence + lead.
    expect(mockDispatchLeadToInstantly).toHaveBeenCalledTimes(1);
    const dispatchArgs = mockDispatchLeadToInstantly.mock.calls[0][0] as {
      sortedSequence: Array<{ step: number; bodyHtml: string; daysSinceLastStep: number }>;
      lead: { email: string; first_name?: string };
      subject: string;
    };
    expect(dispatchArgs.lead.email).toBe("lead@test.com");
    expect(dispatchArgs.lead.first_name).toBe("Lead");
    expect(dispatchArgs.sortedSequence).toHaveLength(1);
    expect(dispatchArgs.subject).toBe("Hi");

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
    expect(mockDbUpdateSet).toHaveBeenCalledWith(
      expect.objectContaining({
        instantlyCampaignId: "inst-camp-NEW",
        notSendingStatus: null,
        notSendingStatusSeenAt: null,
      }),
    );

    // handleCampaignError NOT invoked — lead is alive on the new account.
    expect(mockHandleCampaignError).not.toHaveBeenCalled();

    expect(summary.redispatched).toBe(1);
    expect(summary.cancelled).toBe(0);
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
    mockGetCampaign.mockResolvedValueOnce(SINGLE_STEP_LIVE);
    queueSelectLead();
    queueSelectCosts([]);

    await runRetryStuck();

    const muteCall = mockDbUpdateSet.mock.calls.find((args) => {
      const v = args[0] as Record<string, unknown>;
      return "instantlyCampaignId" in v;
    });
    expect(muteCall).toBeDefined();
    const mutePayload = muteCall![0] as { metadata: { redispatchCount: number; redispatchHistory: unknown[] } };
    expect(mutePayload.metadata.redispatchCount).toBe(2);
    expect(mutePayload.metadata.redispatchHistory).toHaveLength(2);
  });

  it("falls back to handleCampaignError(cancelled) when no healthy account exists", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce(SINGLE_STEP_LIVE);
    queueSelectLead();
    queueSelectCosts([]);

    mockDispatchLeadToInstantly.mockResolvedValueOnce({
      ok: false,
      reason: "no_healthy_account",
    });

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("redispatch_failed: no_healthy_account"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
    expect(summary.cancelled).toBe(1);
    expect(summary.redispatched).toBe(0);
  });

  it("falls back to handleCampaignError(cancelled) when every dispatch attempt hits NSS", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce(SINGLE_STEP_LIVE);
    queueSelectLead();
    queueSelectCosts([]);

    mockDispatchLeadToInstantly.mockResolvedValueOnce({
      ok: false,
      reason: "max_retries_exhausted",
    });

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("redispatch_failed: max_retries_exhausted"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
    expect(summary.cancelled).toBe(1);
    expect(summary.redispatched).toBe(0);
  });

  it("writes not_sending_status diagnostic onto the row even when re-dispatch succeeds", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({
      ...SINGLE_STEP_LIVE,
      not_sending_status: 4,
    });
    queueSelectLead();
    queueSelectCosts([]);

    await runRetryStuck();

    expect(mockDbUpdateSet).toHaveBeenCalledWith(
      expect.objectContaining({
        notSendingStatus: 4,
        notSendingStatusSeenAt: expect.any(Date),
      }),
    );
  });

  it("falls back to cancel when the live campaign has no sequence (cannot rebuild)", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({
      not_sending_status: 4,
      sequences: [],
    });
    queueSelectCosts([]);

    const summary = await runRetryStuck();

    expect(mockDispatchLeadToInstantly).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("no_sequence_on_live_campaign"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
    expect(summary.cancelled).toBe(1);
  });

  it("falls back to cancel when the lead's local profile row is missing", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce(SINGLE_STEP_LIVE);
    // No queueSelectLead() — lookup returns empty array.
    queueSelectCosts([]);

    const summary = await runRetryStuck();

    expect(mockDispatchLeadToInstantly).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      expect.stringContaining("lead_profile_not_found"),
      expect.objectContaining({ terminalStatus: "cancelled" }),
    );
    expect(summary.cancelled).toBe(1);
  });

  it("continues with re-dispatch even when Instantly pause fails", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce(SINGLE_STEP_LIVE);
    mockUpdateCampaignStatus.mockRejectedValueOnce(new Error("instantly 503"));
    queueSelectLead();
    queueSelectCosts([]);

    const summary = await runRetryStuck();

    expect(mockDispatchLeadToInstantly).toHaveBeenCalled();
    expect(summary.redispatched).toBe(1);
  });

  it("counts failures without halting the sweep", async () => {
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
  });

  it("mirrors the lead onto the new Instantly campaign so future re-dispatches can find it", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce(SINGLE_STEP_LIVE);
    queueSelectLead();
    queueSelectCosts([]);

    await runRetryStuck();

    // db.insert called with the new instantlyCampaignId + lead data.
    const leadInsert = mockDbInsertValues.mock.calls.find((args) => {
      const v = args[0] as Record<string, unknown>;
      return v.instantlyCampaignId === "inst-camp-NEW" && v.email === "lead@test.com";
    });
    expect(leadInsert).toBeDefined();
  });
});

/** Concatenate every string fragment in a drizzle SQL query for assertions. */
function chunkText(query: unknown): string {
  if (!query || typeof query !== "object") return String(query);
  const chunks = (query as { queryChunks?: unknown[] }).queryChunks;
  if (!Array.isArray(chunks)) return String(query);
  let out = "";
  for (const c of chunks) {
    if (typeof c === "string") {
      out += c;
      continue;
    }
    if (c && typeof c === "object") {
      const v = (c as { value?: unknown }).value;
      if (typeof v === "string") out += v;
      else if (Array.isArray(v)) out += v.join("");
    }
  }
  return out;
}
