import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockSelectWhere = vi.fn();
vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: () => ({ where: (...a: unknown[]) => mockSelectWhere(...a) }) }),
  },
}));

const mockListEmailsPage = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listEmailsPage: (...a: unknown[]) => mockListEmailsPage(...a),
}));

const mockInsertEmailsBatch = vi.fn();
vi.mock("../../src/lib/bronze", () => ({
  insertEmailsBatch: (...a: unknown[]) => mockInsertEmailsBatch(...a),
}));

import { groupByCampaign, backfillEmails } from "../../src/lib/emails-backfill";
import type { EmailRecord } from "../../src/lib/instantly-client";

const API_KEY = "test-key";

function email(overrides: Partial<EmailRecord> & { id: string }): EmailRecord {
  return {
    campaign_id: "camp-1",
    eaccount: "sender@example.com",
    ue_type: 1,
    step: "1",
    timestamp_email: "2026-08-28T10:00:00.000Z",
    ...overrides,
  } as EmailRecord;
}

beforeEach(() => {
  vi.resetAllMocks();
  // Default: no local campaign rows, and every insert writes every row.
  mockSelectWhere.mockResolvedValue([]);
  mockInsertEmailsBatch.mockImplementation((_c: unknown, _o: unknown, emails: EmailRecord[]) =>
    Promise.resolve(emails.map((e) => ({ id: `row-${e.id}` }))),
  );
});

// ─── Pure grouping ───────────────────────────────────────────────────────────

describe("groupByCampaign", () => {
  it("groups emails by campaign id, preserving order within a group", () => {
    const groups = groupByCampaign([
      email({ id: "a", campaign_id: "c1" }),
      email({ id: "b", campaign_id: "c2" }),
      email({ id: "c", campaign_id: "c1" }),
    ]);

    expect([...groups.keys()]).toEqual(["c1", "c2"]);
    expect(groups.get("c1")?.map((e) => e.id)).toEqual(["a", "c"]);
    expect(groups.get("c2")?.map((e) => e.id)).toEqual(["b"]);
  });

  it("keys campaign-less mail under null rather than inventing a campaign id", () => {
    const groups = groupByCampaign([
      email({ id: "a", campaign_id: undefined }),
      email({ id: "b", campaign_id: "" }),
      email({ id: "c", campaign_id: "c1" }),
    ]);

    expect(groups.get(null)?.map((e) => e.id)).toEqual(["a", "b"]);
    expect(groups.get("c1")?.map((e) => e.id)).toEqual(["c"]);
  });
});

// ─── Sweep ───────────────────────────────────────────────────────────────────

describe("backfillEmails", () => {
  it("walks every page until the cursor is exhausted", async () => {
    mockListEmailsPage
      .mockResolvedValueOnce({ items: [email({ id: "1" })], nextStartingAfter: "cur-1" })
      .mockResolvedValueOnce({ items: [email({ id: "2" })], nextStartingAfter: "cur-2" })
      .mockResolvedValueOnce({ items: [email({ id: "3" })], nextStartingAfter: null });

    const summary = await backfillEmails(API_KEY);

    expect(summary.pages).toBe(3);
    expect(summary.emailsRead).toBe(3);
    expect(summary.emailsStored).toBe(3);
    expect(summary.exhausted).toBe(true);
    // The cursor of page N is what page N+1 asks for.
    expect(mockListEmailsPage.mock.calls[1][1]).toMatchObject({ startingAfter: "cur-1" });
    expect(mockListEmailsPage.mock.calls[2][1]).toMatchObject({ startingAfter: "cur-2" });
  });

  it("terminates on an empty page even when a cursor is still returned", async () => {
    mockListEmailsPage
      .mockResolvedValueOnce({ items: [email({ id: "1" })], nextStartingAfter: "cur-1" })
      .mockResolvedValueOnce({ items: [], nextStartingAfter: "cur-2" });

    const summary = await backfillEmails(API_KEY);

    expect(summary.pages).toBe(2);
    expect(summary.exhausted).toBe(true);
    expect(mockListEmailsPage).toHaveBeenCalledTimes(2);
  });

  it("stops at maxPages and reports the sweep as NOT exhausted", async () => {
    mockListEmailsPage.mockResolvedValue({
      items: [email({ id: "x" })],
      nextStartingAfter: "cur",
    });

    const summary = await backfillEmails(API_KEY, { maxPages: 2 });

    expect(summary.pages).toBe(2);
    expect(summary.exhausted).toBe(false);
    expect(mockListEmailsPage).toHaveBeenCalledTimes(2);
  });

  it("stores each campaign's emails under that campaign's own org", async () => {
    mockListEmailsPage.mockResolvedValueOnce({
      items: [
        email({ id: "1", campaign_id: "c1" }),
        email({ id: "2", campaign_id: "c2" }),
      ],
      nextStartingAfter: null,
    });
    mockSelectWhere.mockResolvedValue([
      { instantlyCampaignId: "c1", orgId: "org-1" },
      { instantlyCampaignId: "c2", orgId: "org-2" },
    ]);

    await backfillEmails(API_KEY);

    expect(mockInsertEmailsBatch).toHaveBeenCalledWith("c1", "org-1", [
      expect.objectContaining({ id: "1" }),
    ]);
    expect(mockInsertEmailsBatch).toHaveBeenCalledWith("c2", "org-2", [
      expect.objectContaining({ id: "2" }),
    ]);
  });

  it("stores campaign-less mail with a null campaign id and a null org", async () => {
    mockListEmailsPage.mockResolvedValueOnce({
      items: [email({ id: "1", campaign_id: undefined })],
      nextStartingAfter: null,
    });

    const summary = await backfillEmails(API_KEY);

    expect(mockInsertEmailsBatch).toHaveBeenCalledWith(null, null, [
      expect.objectContaining({ id: "1" }),
    ]);
    expect(summary.campaignlessRead).toBe(1);
  });

  it("stores a null org for a campaign we hold no local row for", async () => {
    mockListEmailsPage.mockResolvedValueOnce({
      items: [email({ id: "1", campaign_id: "unknown" })],
      nextStartingAfter: null,
    });
    mockSelectWhere.mockResolvedValue([]);

    await backfillEmails(API_KEY);

    expect(mockInsertEmailsBatch).toHaveBeenCalledWith("unknown", null, [
      expect.objectContaining({ id: "1" }),
    ]);
  });

  it("counts inbound mail separately — that is the half the deletion destroys", async () => {
    mockListEmailsPage.mockResolvedValueOnce({
      items: [
        email({ id: "1", ue_type: 1 }),
        email({ id: "2", ue_type: 2 }),
        email({ id: "3", ue_type: 2 }),
      ],
      nextStartingAfter: null,
    });

    const summary = await backfillEmails(API_KEY);

    expect(summary.emailsRead).toBe(3);
    expect(summary.inboundRead).toBe(2);
  });

  it("counts only NEWLY written rows as stored, so a re-run reports what it added", async () => {
    mockListEmailsPage.mockResolvedValueOnce({
      items: [email({ id: "1" }), email({ id: "2" })],
      nextStartingAfter: null,
    });
    // Both already mirrored: the conflict clause returns no rows.
    mockInsertEmailsBatch.mockResolvedValue([]);

    const summary = await backfillEmails(API_KEY);

    expect(summary.emailsRead).toBe(2);
    expect(summary.emailsStored).toBe(0);
  });

  it("resumes from a supplied cursor instead of re-walking from the newest email", async () => {
    mockListEmailsPage.mockResolvedValueOnce({
      items: [email({ id: "1" })],
      nextStartingAfter: null,
    });

    await backfillEmails(API_KEY, { startingAfter: "cur-500" });

    expect(mockListEmailsPage.mock.calls[0][1]).toMatchObject({ startingAfter: "cur-500" });
  });

  it("reports the cursor to resume from when maxPages cuts the walk short", async () => {
    mockListEmailsPage
      .mockResolvedValueOnce({ items: [email({ id: "1" })], nextStartingAfter: "cur-1" })
      .mockResolvedValueOnce({ items: [email({ id: "2" })], nextStartingAfter: "cur-2" });

    const summary = await backfillEmails(API_KEY, { maxPages: 2 });

    expect(summary.exhausted).toBe(false);
    expect(summary.nextCursor).toBe("cur-2");
  });

  it("reports a null cursor once the list is exhausted — there is nothing to resume", async () => {
    mockListEmailsPage.mockResolvedValueOnce({
      items: [email({ id: "1" })],
      nextStartingAfter: null,
    });

    const summary = await backfillEmails(API_KEY);

    expect(summary.exhausted).toBe(true);
    expect(summary.nextCursor).toBeNull();
  });

  it("keeps the supplied cursor as the resume point when the very first page errors", async () => {
    // A run killed immediately must not report "start from the newest email".
    mockListEmailsPage.mockResolvedValueOnce({ items: [], nextStartingAfter: null });

    const summary = await backfillEmails(API_KEY, { startingAfter: "cur-500" });

    expect(summary.nextCursor).toBeNull();
    expect(summary.exhausted).toBe(true);
  });

  it("fails loud when a page errors — a partial copy must not read as a clean run", async () => {
    mockListEmailsPage
      .mockResolvedValueOnce({ items: [email({ id: "1" })], nextStartingAfter: "cur-1" })
      .mockRejectedValueOnce(new Error("Instantly 429"));

    await expect(backfillEmails(API_KEY)).rejects.toThrow("Instantly 429");
  });

  it("fails loud when the bronze write errors", async () => {
    mockListEmailsPage.mockResolvedValueOnce({
      items: [email({ id: "1" })],
      nextStartingAfter: null,
    });
    mockInsertEmailsBatch.mockRejectedValue(new Error("insert failed"));

    await expect(backfillEmails(API_KEY)).rejects.toThrow("insert failed");
  });
});
