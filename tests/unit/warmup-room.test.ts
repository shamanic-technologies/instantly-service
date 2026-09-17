import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockInsert = vi.fn(() => ({ values: vi.fn(async () => undefined) }));
vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockExecute(...a),
    insert: (...a: unknown[]) => mockInsert(...(a as [])),
  },
}));
vi.mock("../../src/lib/recent-send-volume", () => ({
  fetchRecentDailyVolume: vi.fn(async () => new Map()),
  sustainedForMailbox: vi.fn(() => 0),
}));
vi.mock("../../src/lib/self-send/mailbox-credentials", () => ({
  loadMailboxLogins: vi.fn(async () => new Map<string, string>()),
  loginFor: vi.fn((email: string) => email),
}));
vi.mock("../../src/lib/seed-placement/credentials", () => ({
  loadSeedCredentialResolver: vi.fn(async () => () => null),
}));
vi.mock("../../src/lib/self-send/smtp", () => ({
  dispatchMessage: vi.fn(),
  SmtpDispatchError: class extends Error {},
  classifyDispatchFailure: vi.fn(() => "transient"),
}));
vi.mock("../../src/lib/warmup/message", () => ({
  buildWarmupMessage: vi.fn(async () => ({ subject: "s", text: "t" })),
}));

import { runWarmupMesh } from "../../src/lib/warmup/run";

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

describe("loadRoom — a credentialed mailbox Instantly no longer lists can still SEND", () => {
  beforeEach(() => {
    mockExecute.mockReset();
    mockExecute.mockResolvedValue({ rows: [] });
  });

  it("does NOT filter on absent_since", async () => {
    // `absent_since` records ONE fact: the account left the Instantly
    // workspace. The mailbox is still at Gandi and we still hold its password.
    // Filtering on it dropped the mailbox from the room while
    // `partnerCandidates` (which reads the credential map) kept choosing it as
    // a RECEIVER — so it accumulated inbound warmup and sent none of its own.
    // Measured 2026-09-17 on `kevin@distribute.you`: 67 received, 0 sent.
    await runWarmupMesh();
    const roomSql = mockExecute.mock.calls
      .map((c) => sqlTextOf(c[0]))
      .find((t) => t.includes('a.daily_limit'));
    expect(roomSql).toBeDefined();
    expect(roomSql).not.toContain("absent_since");
  });

  it("still reads the room FROM instantly_accounts, so an account we hold no row for gets none", async () => {
    await runWarmupMesh();
    const roomSql = mockExecute.mock.calls
      .map((c) => sqlTextOf(c[0]))
      .find((t) => t.includes('a.daily_limit'));
    expect(roomSql).toContain("FROM instantly_accounts a");
  });
});
