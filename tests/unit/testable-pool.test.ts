import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockExecute(...a) },
}));
vi.mock("../../src/lib/instantly-client", () => ({
  setWarmupDailyLimit: vi.fn(),
  setDailyLimit: vi.fn(),
  listAccounts: vi.fn(async () => []),
}));

import {
  fetchTestablePoolEmails,
  TESTABLE_MIN_AGE_DAYS,
} from "../../src/lib/account-lifecycle-sync";

/** Reassemble the parameterized SQL the way drizzle would render it. */
function sqlTextOf(call: unknown): string {
  const q = call as { queryChunks?: unknown[] };
  return (q.queryChunks ?? [])
    .map((c) => {
      const chunk = c as { value?: unknown };
      return Array.isArray(chunk?.value) ? chunk.value.join("") : "";
    })
    .join("");
}

describe("fetchTestablePoolEmails — weekly placement-test seeding", () => {
  beforeEach(() => {
    mockExecute.mockReset();
    mockExecute.mockResolvedValue({ rows: [] });
  });

  it("the age floor is one week", () => {
    expect(TESTABLE_MIN_AGE_DAYS).toBe(7);
  });

  it("seeds in_recovery AND in_production — in_recovery is what breaks the bootstrap deadlock", async () => {
    await fetchTestablePoolEmails();
    const text = sqlTextOf(mockExecute.mock.calls[0][0]);
    expect(text).toContain("lifecycle_status IN ('in_recovery', 'in_production')");
  });

  it("excludes accounts younger than the age floor, keeping undated ones", async () => {
    await fetchTestablePoolEmails();
    const text = sqlTextOf(mockExecute.mock.calls[0][0]);
    expect(text).toContain("timestamp_created IS NULL");
    expect(text).toContain("timestamp_created <= now() - make_interval(days =>");
  });

  it("returns the emails and drops blanks", async () => {
    mockExecute.mockResolvedValue({
      rows: [{ email: "a@x.com" }, { email: "" }, { email: "b@x.com" }],
    });
    expect(await fetchTestablePoolEmails()).toEqual(["a@x.com", "b@x.com"]);
  });
});
