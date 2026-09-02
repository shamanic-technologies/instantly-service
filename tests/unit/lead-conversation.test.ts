import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockDbExecute(...a),
    insert: () => ({ values: vi.fn() }),
  },
}));
vi.mock("../../src/db/schema", () => ({ smtpDispatchRaw: { id: "id" } }));

const mockListEmails = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listEmails: (...a: unknown[]) => mockListEmails(...a),
  getAccount: vi.fn(),
  replyToEmail: vi.fn(),
}));

const mockResolveKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...a: unknown[]) => mockResolveKey(...a),
}));

const mockFetchSelfSendThread = vi.fn();
vi.mock("../../src/lib/self-send/thread", () => ({
  fetchSelfSendThread: (...a: unknown[]) => mockFetchSelfSendThread(...a),
}));

vi.mock("../../src/lib/self-send/mailbox-credentials", () => ({
  resolveMailboxCredential: vi.fn(),
}));
vi.mock("../../src/lib/self-send/smtp", () => ({ dispatchMessage: vi.fn() }));

import {
  fetchLeadConversation,
  LeadConversationError,
} from "../../src/lib/lead-conversation";
import type { EmailRecord } from "../../src/lib/instantly-client";

/** Recursively extract SQL text fragments from a drizzle SQL object. */
function extractSqlText(obj: unknown): string {
  if (typeof obj === "string") return obj;
  if (obj == null) return "";
  if (Array.isArray(obj)) return obj.map(extractSqlText).join("");
  if (typeof obj === "object") {
    const o = obj as Record<string, unknown>;
    if (Array.isArray(o.value)) return o.value.join("");
    if (Array.isArray(o.queryChunks)) return extractSqlText(o.queryChunks);
    return Object.values(o).map(extractSqlText).join("");
  }
  return "";
}

/** node-postgres returns a QueryResult OBJECT, never a bare array. */
function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

const INPUT = {
  orgId: "org-1",
  userId: "user-1",
  campaignId: "camp-1",
  leadEmail: "Alice@Media.com",
};

function campaignRow(over: Record<string, unknown> = {}) {
  return {
    instantlyCampaignId: "ic-1",
    leadEmail: "alice@media.com",
    accountEmail: "amy@boostdistribute.com",
    sendTransport: "instantly",
    ...over,
  };
}

function email(over: Partial<EmailRecord>): EmailRecord {
  return {
    id: "e1",
    campaign_id: "ic-1",
    lead: "alice@media.com",
    lead_id: null,
    eaccount: "amy@boostdistribute.com",
    ue_type: 1,
    step: "1",
    timestamp_email: "2026-09-01T10:00:00.000Z",
    ...over,
  } as EmailRecord;
}

beforeEach(() => {
  vi.clearAllMocks();
  mockResolveKey.mockResolvedValue({ key: "api-key" });
});

describe("fetchLeadConversation — Instantly transport", () => {
  it("returns what the prospect wrote AND what we sent, oldest first, as text", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([
      email({
        id: "e2",
        ue_type: 2,
        from_address_email: "alice@media.com",
        to_address_email_list: "amy@boostdistribute.com",
        subject: "Re: quick question",
        timestamp_email: "2026-09-02T09:00:00.000Z",
        body: { text: "Sure — what does it cost?" },
      } as Partial<EmailRecord>),
      email({
        id: "e1",
        ue_type: 1,
        from_address_email: "amy@boostdistribute.com",
        to_address_email_list: "alice@media.com",
        subject: "quick question",
        timestamp_email: "2026-09-01T10:00:00.000Z",
        body: { html: "<p>Hi Alice,</p><p>worth a chat?</p>" },
      } as Partial<EmailRecord>),
    ]);

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.transport).toBe("instantly");
    expect(conv.instantlyCampaignId).toBe("ic-1");
    expect(conv.messageCount).toBe(2);
    // Oldest first — ours, then theirs.
    expect(conv.messages.map((m) => m.direction)).toEqual(["outbound", "inbound"]);
    // The prospect's own words are present.
    expect(conv.messages[1].text).toBe("Sure — what does it cost?");
    expect(conv.messages[1].from).toBe("alice@media.com");
    expect(conv.messages[1].at).toBe("2026-09-02T09:00:00.000Z");
    // Ours is readable TEXT, not the stored HTML.
    expect(conv.messages[0].text).toContain("worth a chat?");
    expect(conv.messages[0].text).not.toContain("<p>");
  });

  it("does NOT start at the prospect's reply — our own words are what they answered", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([
      email({ id: "s1", ue_type: 1, timestamp_email: "2026-09-01T10:00:00.000Z" }),
      email({ id: "s2", ue_type: 1, timestamp_email: "2026-09-04T10:00:00.000Z" }),
      email({
        id: "r1",
        ue_type: 2,
        timestamp_email: "2026-09-05T10:00:00.000Z",
        body: { text: "ok" },
      } as Partial<EmailRecord>),
    ]);

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.messages).toHaveLength(3);
    expect(conv.messages[0].direction).toBe("outbound");
  });

  it("scopes the lookup to the caller's org and matches the email case-insensitively", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([]);

    await fetchLeadConversation(INPUT);

    const text = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("c.org_id =");
    expect(text).toContain("lower(c.lead_email) = lower(");
  });

  it("returns the STORED lead email, not the caller's spelling", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([]);

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.leadEmail).toBe("alice@media.com");
  });
});

describe("fetchLeadConversation — self-send transport", () => {
  it("reads the thread out of bronze, in the same shape", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([campaignRow({ instantlyCampaignId: "self:abc", sendTransport: "smtp" })]),
    );
    mockFetchSelfSendThread.mockResolvedValue([
      {
        direction: "outbound",
        from: "amy@boostdistribute.com",
        to: "alice@media.com",
        date: "2026-09-01T10:00:00.000Z",
        subject: "quick question",
        bodyText: "worth a chat?",
      },
      {
        direction: "inbound",
        from: "alice@media.com",
        to: "amy@boostdistribute.com",
        date: "2026-09-02T09:00:00.000Z",
        subject: "Re: quick question",
        bodyText: "Sure — what does it cost?",
      },
    ]);

    const conv = await fetchLeadConversation(INPUT);

    expect(mockFetchSelfSendThread).toHaveBeenCalledWith("self:abc");
    // Instantly is never asked about a sequence it never carried.
    expect(mockListEmails).not.toHaveBeenCalled();
    expect(mockResolveKey).not.toHaveBeenCalled();
    expect(conv.transport).toBe("smtp");
    expect(conv.messages.map((m) => m.direction)).toEqual(["outbound", "inbound"]);
    expect(conv.messages[1].text).toBe("Sure — what does it cost?");
  });
});

describe("fetchLeadConversation — refusals", () => {
  it("a conversation nobody has on record is a 404, NOT an empty list", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([]));

    await expect(fetchLeadConversation(INPUT)).rejects.toMatchObject({
      code: "campaign_not_found",
      status: 404,
    });
    expect(mockListEmails).not.toHaveBeenCalled();
  });

  it("a sequence that exists with nothing exchanged is a 200 with an empty list", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([]);

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.messageCount).toBe(0);
    expect(conv.messages).toEqual([]);
  });

  it("a thread we hold but cannot read fails loud — never an empty list", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockRejectedValue(new Error("Instantly 503"));

    const err = await fetchLeadConversation(INPUT).catch((e) => e);
    expect(err).toBeInstanceOf(LeadConversationError);
    expect(err.code).toBe("thread_unavailable");
    expect(err.status).toBe(502);
    expect(err.message).toContain("Instantly 503");
  });

  it("a stored thread that cannot be read fails loud too", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([campaignRow({ sendTransport: "smtp" })]),
    );
    mockFetchSelfSendThread.mockRejectedValue(new Error("connection reset"));

    await expect(fetchLeadConversation(INPUT)).rejects.toMatchObject({
      code: "thread_unavailable",
      status: 502,
    });
  });
});
