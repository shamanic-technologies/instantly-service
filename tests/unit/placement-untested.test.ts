import { describe, it, expect, vi, beforeEach } from "vitest";

// ── Mock the DB connection ───────────────────────────────────────────────────
const mockExecute = vi.fn();
const mockInsertOnConflict = vi.fn(async () => undefined);
const mockInsertValues = vi.fn(() => ({ onConflictDoUpdate: mockInsertOnConflict }));

vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockExecute(...a),
    insert: () => ({ values: (...a: unknown[]) => mockInsertValues(...a) }),
  },
}));
vi.mock("../../src/db/schema", () => ({
  instantlyPlacementTestsRaw: { testId: "test_id" },
  instantlyPlacementAnalyticsRaw: { analyticsId: "analytics_id" },
  instantlyPlacementResults: {
    testId: "test_id",
    accountEmail: "account_email",
    recipientEsp: "recipient_esp",
  },
}));

const mockCreateTest = vi.fn();
const mockEspOptions = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listInboxPlacementTests: vi.fn(async () => []),
  listInboxPlacementAnalytics: vi.fn(async () => []),
  createInboxPlacementTest: (...a: unknown[]) => mockCreateTest(...a),
  getEmailServiceProviderOptions: (...a: unknown[]) => mockEspOptions(...a),
}));

vi.mock("../../src/lib/account-lifecycle-sync", () => ({
  fetchTestablePoolEmails: vi.fn(async () => []),
}));

import {
  runUntestedPlacementTest,
  UNTESTED_PLACEMENT_TEST_CODE,
  UNTESTED_RETEST_SUPPRESSION_HOURS,
} from "../../src/lib/placement-sync";

beforeEach(() => {
  vi.clearAllMocks();
  mockEspOptions.mockResolvedValue([
    { esp: "Google", label: "gmail" },
    { esp: "Outlook", label: "outlook" },
    { esp: "Yahoo", label: "yahoo" },
  ]);
});

describe("runUntestedPlacementTest", () => {
  it("suppression window is 48h — long enough for results to land before a re-test", () => {
    expect(UNTESTED_RETEST_SUPPRESSION_HOURS).toBe(48);
  });

  it("no never-tested account → NO Instantly call, no quota spent, created 0", async () => {
    mockExecute.mockResolvedValue({ rows: [] });

    const summary = await runUntestedPlacementTest("api-key");

    expect(summary).toEqual({ created: 0, testCode: null, recipientEsps: [], senderCount: 0 });
    expect(mockEspOptions).not.toHaveBeenCalled();
    expect(mockCreateTest).not.toHaveBeenCalled();
    expect(mockInsertValues).not.toHaveBeenCalled();
  });

  it("seeds ONLY the never-tested accounts, Google + Outlook, run_immediately", async () => {
    mockExecute.mockResolvedValue({
      rows: [{ email: "fresh1@dfy.com" }, { email: "fresh2@dfy.com" }],
    });
    mockCreateTest.mockResolvedValue({ id: "test-1", test_code: UNTESTED_PLACEMENT_TEST_CODE });

    const summary = await runUntestedPlacementTest("api-key");

    expect(summary).toEqual({
      created: 1,
      testCode: UNTESTED_PLACEMENT_TEST_CODE,
      recipientEsps: ["Google", "Outlook"],
      senderCount: 2,
    });
    expect(mockCreateTest).toHaveBeenCalledTimes(1);
    const body = mockCreateTest.mock.calls[0][1] as Record<string, unknown>;
    expect(body.emails).toEqual(["fresh1@dfy.com", "fresh2@dfy.com"]);
    expect(body.type).toBe(1);
    expect(body.run_immediately).toBe(true);
    expect(body.recipients_labels).toHaveLength(2); // Yahoo excluded
  });

  it("writes the created test to BRONZE with the senders — the 48h re-test suppression", async () => {
    mockExecute.mockResolvedValue({ rows: [{ email: "fresh1@dfy.com" }] });
    // The create RESPONSE does not echo `emails` back — bronze must carry them anyway.
    mockCreateTest.mockResolvedValue({ id: "test-1", test_code: UNTESTED_PLACEMENT_TEST_CODE });

    await runUntestedPlacementTest("api-key");

    expect(mockInsertValues).toHaveBeenCalledTimes(1);
    const row = mockInsertValues.mock.calls[0][0] as Record<string, unknown>;
    expect(row.testId).toBe("test-1");
    expect((row.payload as Record<string, unknown>).emails).toEqual(["fresh1@dfy.com"]);
    // Upsert, so a later `sync` overwrites bronze with Instantly's own payload.
    expect(mockInsertOnConflict).toHaveBeenCalledTimes(1);
  });

  it("fails loud when Instantly rejects the create (402 quota / 400)", async () => {
    mockExecute.mockResolvedValue({ rows: [{ email: "fresh1@dfy.com" }] });
    mockCreateTest.mockRejectedValue(new Error("Instantly 402: quota exceeded"));

    await expect(runUntestedPlacementTest("api-key")).rejects.toThrow(/402/);
    expect(mockInsertValues).not.toHaveBeenCalled();
  });

  it("selection SQL gates on testable lifecycle + zero silver rows + the suppression window", async () => {
    mockExecute.mockResolvedValue({ rows: [] });

    await runUntestedPlacementTest("api-key");

    const query = mockExecute.mock.calls[0][0] as { queryChunks?: unknown[] };
    const text = JSON.stringify(query);
    expect(text).toContain("in_recovery");
    expect(text).toContain("in_production");
    expect(text).toContain("instantly_placement_results");
    expect(text).toContain("instantly_placement_tests_raw");
    expect(text).toContain("jsonb_exists");
    expect(text).toContain("make_interval");
  });
});
