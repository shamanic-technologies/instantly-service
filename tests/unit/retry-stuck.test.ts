import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────
//
// db.execute is called multiple times per sweep:
//   1. SELECT pg_try_advisory_lock(...) AS locked
//   2. SELECT ... FROM instantly_campaigns ... LIMIT 500
//   3. UPDATE instantly_campaigns SET not_sending_status=..., seen_at=... (per cancelled row)
//   4. SELECT pg_advisory_unlock(...)
//
// Tests use `setupSweep(rows)` to queue (1) + (2) in order. The catch-all
// `mockResolvedValue({ rows: [] })` from beforeEach handles (3) + (4).

const mockDbExecute = vi.fn();
const mockDbUpdate = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockDbExecute(...args),
    // The cancel branch ALSO does a drizzle ORM `db.update(...).set(...).where(...)`
    // to write not_sending_status — separate from db.execute. Track via mockDbUpdate.
    update: () => ({
      set: (v: unknown) => ({
        where: () => {
          mockDbUpdate(v);
          return Promise.resolve([{}]);
        },
      }),
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

import {
  runRetryStuck,
  MAX_RETRIES,
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
    metadata: null,
    ...extra,
  };
}

/** Queue the two ordered db.execute responses for a normal sweep entry. */
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
    // Default: subsequent execute() calls (UPDATE, UNLOCK) return empty rows.
    mockDbExecute.mockResolvedValue({ rows: [] });
    mockResolveKey.mockResolvedValue({ key: "fake-api-key" });
    mockGetCampaign.mockResolvedValue({});
    mockUpdateCampaignStatus.mockResolvedValue({});
    mockHandleCampaignError.mockResolvedValue(undefined);
  });

  it("cancels stuck row when not_sending_status is set", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({
      not_sending_status: { reason: "account_disconnected" },
    });

    const summary = await runRetryStuck();

    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith(
      "fake-api-key",
      "inst-camp-1",
      "paused",
    );
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-camp-1",
      'not_sending_status: {"reason":"account_disconnected"}',
      expect.objectContaining({
        terminalStatus: "cancelled",
        extraMetadata: expect.objectContaining({
          notSendingStatus: { reason: "account_disconnected" },
          retryCount: 1,
        }),
      }),
    );
    expect(summary.cancelled).toBe(1);
    expect(summary.scanned).toBe(1);
    expect(summary.skipped).toBeUndefined();
  });

  it("increments retryCount based on existing metadata.retryCount", async () => {
    setupSweep([row({ metadata: { retryCount: 1 } })]);
    mockGetCampaign.mockResolvedValueOnce({ not_sending_status: "stuck" });

    await runRetryStuck();

    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      expect.objectContaining({
        extraMetadata: expect.objectContaining({ retryCount: 2 }),
      }),
    );
  });

  it("skips when not_sending_status is null", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({ not_sending_status: null });

    const summary = await runRetryStuck();

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(summary.stillSending).toBe(1);
    expect(summary.cancelled).toBe(0);
  });

  it("skips when not_sending_status is missing (undefined)", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({});

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(summary.stillSending).toBe(1);
  });

  it(`caps retries at MAX_RETRIES=${MAX_RETRIES}`, async () => {
    setupSweep([row({ metadata: { retryCount: MAX_RETRIES } })]);

    const summary = await runRetryStuck();

    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(summary.capped).toBe(1);
  });

  it("skips org when key resolution fails", async () => {
    setupSweep([row()]);
    mockResolveKey.mockRejectedValueOnce(new MockKeyServiceError(404, "key not configured"));

    const summary = await runRetryStuck();

    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(summary.skippedNoKey).toBe(1);
  });

  it("counts failures without halting the sweep", async () => {
    setupSweep([
      row({ id: "a", instantlyCampaignId: "inst-a" }),
      row({ id: "b", instantlyCampaignId: "inst-b" }),
    ]);
    mockGetCampaign
      .mockRejectedValueOnce(new Error("instantly 500"))
      .mockResolvedValueOnce({ not_sending_status: "stuck" });

    const summary = await runRetryStuck();

    expect(summary.scanned).toBe(2);
    expect(summary.failed).toBe(1);
    expect(summary.cancelled).toBe(1);
    expect(mockHandleCampaignError).toHaveBeenCalledWith(
      "inst-b",
      expect.any(String),
      expect.any(Object),
    );
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

  it("continues with cost cancel even when Instantly pause fails", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({ not_sending_status: "stuck" });
    mockUpdateCampaignStatus.mockRejectedValueOnce(new Error("instantly 503"));

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).toHaveBeenCalled();
    expect(summary.cancelled).toBe(1);
  });

  // ─── New: concurrency lock ─────────────────────────────────────────────────

  it("short-circuits with skipped='sweep_in_progress' when the advisory lock is held", async () => {
    setupSweep([], { lockAcquired: false });

    const summary = await runRetryStuck();

    expect(summary.skipped).toBe("sweep_in_progress");
    expect(summary.scanned).toBe(0);
    expect(summary.cancelled).toBe(0);
    // Lock-blocked sweep must NOT proceed to query stuck rows or hit Instantly.
    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(mockResolveKey).not.toHaveBeenCalled();
  });

  it("two sequential sweeps: first cancels, second short-circuits when lock is contended", async () => {
    // First call acquires the lock and processes its stuck row.
    setupSweep([row({ instantlyCampaignId: "inst-a" })]);
    mockGetCampaign.mockResolvedValueOnce({ not_sending_status: "x" });
    const sumA = await runRetryStuck();

    // Second call: simulate the lock still being held by another worker.
    setupSweep([], { lockAcquired: false });
    const sumB = await runRetryStuck();

    expect(sumA.skipped).toBeUndefined();
    expect(sumA.scanned).toBe(1);
    expect(sumA.cancelled).toBe(1);

    expect(sumB.skipped).toBe("sweep_in_progress");
    expect(sumB.scanned).toBe(0);
    expect(sumB.cancelled).toBe(0);

    expect(mockHandleCampaignError).toHaveBeenCalledTimes(1);
  });

  // ─── New: not_sending_status column populated on cancel ────────────────────

  it("writes not_sending_status + seen-at onto the row before cancel", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({ not_sending_status: 4 });

    await runRetryStuck();

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        notSendingStatus: 4,
        notSendingStatusSeenAt: expect.any(Date),
      }),
    );
  });

  it("stores numeric not_sending_status verbatim; non-numeric → null on the column", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({
      not_sending_status: { reason: "weird" },
    });

    await runRetryStuck();

    expect(mockDbUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        // Object diagnostic → column stays null (column is integer); metadata still
        // carries the full object via extraMetadata.
        notSendingStatus: null,
        notSendingStatusSeenAt: expect.any(Date),
      }),
    );
  });

  it("does NOT write nss column when row is still sending", async () => {
    setupSweep([row()]);
    mockGetCampaign.mockResolvedValueOnce({ not_sending_status: null });

    await runRetryStuck();

    expect(mockDbUpdate).not.toHaveBeenCalled();
  });

  // ─── New: row cap + ordering ───────────────────────────────────────────────

  it("limits work to MAX_ROWS_PER_RUN rows in a single sweep", async () => {
    // Mock returns 500 rows even if more were available — the SELECT's LIMIT
    // is what enforces the cap (asserted separately below via SQL chunks).
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

    // db.execute calls in order: [0] lock acquire, [1] select, [2] unlock.
    const selectCall = mockDbExecute.mock.calls[1]?.[0];
    expect(selectCall).toBeDefined();
    const text = chunkText(selectCall);
    expect(text).toMatch(/ORDER BY created_at ASC/);
    expect(text).toMatch(/LIMIT/);
  });

  // ─── Batch sizing constant is exposed ──────────────────────────────────────

  it("exposes BATCH_SIZE > 1 so per-tick parallelism is on", () => {
    expect(BATCH_SIZE).toBeGreaterThan(1);
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

