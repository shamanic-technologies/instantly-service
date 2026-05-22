import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockDbExecute(...args),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { id: "id", instantlyCampaignId: "instantly_campaign_id" },
}));

const mockResolveKey = vi.fn();
const mockKeyServiceError = class KeyServiceError extends Error {
  statusCode: number;
  constructor(statusCode: number, message: string) {
    super(message);
    this.name = "KeyServiceError";
    this.statusCode = statusCode;
  }
};

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveKey(...args),
  KeyServiceError: mockKeyServiceError,
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

// Import under test AFTER mocks
import { runRetryStuck, MAX_RETRIES } from "../../src/lib/retry-stuck";

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

function mockSelect(rows: Array<Record<string, unknown>>) {
  // drizzle's pg pool returns { rows } shape — runRetryStuck handles both.
  mockDbExecute.mockResolvedValueOnce({ rows });
}

describe("runRetryStuck", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockResolveKey.mockResolvedValue({ key: "fake-api-key" });
    mockGetCampaign.mockResolvedValue({});
    mockUpdateCampaignStatus.mockResolvedValue({});
    mockHandleCampaignError.mockResolvedValue(undefined);
  });

  it("cancels stuck row when not_sending_status is set", async () => {
    mockSelect([row()]);
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
  });

  it("increments retryCount based on existing metadata.retryCount", async () => {
    mockSelect([row({ metadata: { retryCount: 1 } })]);
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
    mockSelect([row()]);
    mockGetCampaign.mockResolvedValueOnce({ not_sending_status: null });

    const summary = await runRetryStuck();

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(summary.stillSending).toBe(1);
    expect(summary.cancelled).toBe(0);
  });

  it("skips when not_sending_status is missing (undefined)", async () => {
    mockSelect([row()]);
    mockGetCampaign.mockResolvedValueOnce({});

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(summary.stillSending).toBe(1);
  });

  it(`caps retries at MAX_RETRIES=${MAX_RETRIES}`, async () => {
    mockSelect([row({ metadata: { retryCount: MAX_RETRIES } })]);

    const summary = await runRetryStuck();

    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(summary.capped).toBe(1);
  });

  it("skips org when key resolution fails", async () => {
    mockSelect([row()]);
    mockResolveKey.mockRejectedValueOnce(new mockKeyServiceError(404, "key not configured"));

    const summary = await runRetryStuck();

    expect(mockGetCampaign).not.toHaveBeenCalled();
    expect(mockHandleCampaignError).not.toHaveBeenCalled();
    expect(summary.skippedNoKey).toBe(1);
  });

  it("counts failures without halting the sweep", async () => {
    mockSelect([row({ id: "a", instantlyCampaignId: "inst-a" }), row({ id: "b", instantlyCampaignId: "inst-b" })]);
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
    mockSelect([
      row({ id: "a", orgId: "org-1" }),
      row({ id: "b", orgId: "org-1" }),
      row({ id: "c", orgId: "org-2", instantlyCampaignId: "inst-c" }),
    ]);
    mockGetCampaign.mockResolvedValue({ not_sending_status: null });

    await runRetryStuck();

    expect(mockResolveKey).toHaveBeenCalledTimes(2);
  });

  it("calls db.execute once per invocation (passes opts.all through SQL)", async () => {
    mockSelect([]);
    await runRetryStuck({ all: true });
    expect(mockDbExecute).toHaveBeenCalledTimes(1);

    mockSelect([]);
    await runRetryStuck({ all: false });
    expect(mockDbExecute).toHaveBeenCalledTimes(2);
  });

  it("continues with cost cancel even when Instantly pause fails", async () => {
    mockSelect([row()]);
    mockGetCampaign.mockResolvedValueOnce({ not_sending_status: "stuck" });
    mockUpdateCampaignStatus.mockRejectedValueOnce(new Error("instantly 503"));

    const summary = await runRetryStuck();

    expect(mockHandleCampaignError).toHaveBeenCalled();
    expect(summary.cancelled).toBe(1);
  });
});
