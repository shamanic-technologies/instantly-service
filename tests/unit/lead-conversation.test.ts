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

const mockInsertEmailsBatch = vi.fn();
vi.mock("../../src/lib/bronze", () => ({
  insertEmailsBatch: (...a: unknown[]) => mockInsertEmailsBatch(...a),
}));

const mockFetchSelfSendThread = vi.fn();
vi.mock("../../src/lib/self-send/thread", () => ({
  fetchSelfSendThread: (...a: unknown[]) => mockFetchSelfSendThread(...a),
}));

vi.mock("../../src/lib/self-send/mailbox-credentials", () => ({
  resolveMailboxCredential: vi.fn(),
}));
vi.mock("../../src/lib/self-send/smtp", () => ({ dispatchMessage: vi.fn() }));

const mockGetCampaignFamily = vi.fn();
vi.mock("../../src/lib/campaign-client", () => ({
  getCampaignFamily: (...a: unknown[]) => mockGetCampaignFamily(...a),
}));

import {
  fetchLeadConversation,
  mergeConversationMessages,
  LeadConversationError,
  MAX_CONVERSATION_SEQUENCES,
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
    campaignId: "camp-1",
    createdAt: "2026-09-01 10:00:00",
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

/**
 * Queue the `db.execute` results this read makes, in order: the campaign
 * lookup, then (Instantly transport only) the bronze mirror, then the
 * exchanged-mail evidence. Anything past the queue answers empty.
 */
function queueDb(...results: unknown[][]) {
  mockDbExecute.mockReset();
  for (const rows of results) mockDbExecute.mockResolvedValueOnce(pgResult(rows));
  mockDbExecute.mockResolvedValue(pgResult([]));
}

/** A bronze `instantly_emails_raw` row — the payload IS the Instantly record. */
function mirrorRow(record: EmailRecord) {
  return { payload: record };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockResolveKey.mockResolvedValue({ key: "api-key" });
  mockInsertEmailsBatch.mockResolvedValue([]);
  // Default: the campaign is a single stored row — the pre-family behaviour.
  mockGetCampaignFamily.mockResolvedValue(["camp-1"]);
});

describe("fetchLeadConversation — Instantly transport", () => {
  const OUTBOUND = email({
    id: "e1",
    ue_type: 1,
    from_address_email: "amy@boostdistribute.com",
    to_address_email_list: "alice@media.com",
    subject: "quick question",
    timestamp_email: "2026-09-01T10:00:00.000Z",
    body: { html: "<p>Hi Alice,</p><p>worth a chat?</p>" },
  } as Partial<EmailRecord>);
  const INBOUND = email({
    id: "e2",
    ue_type: 2,
    from_address_email: "alice@media.com",
    to_address_email_list: "amy@boostdistribute.com",
    subject: "Re: quick question",
    timestamp_email: "2026-09-02T09:00:00.000Z",
    body: { text: "Sure — what does it cost?" },
  } as Partial<EmailRecord>);

  it("returns what the prospect wrote AND what we sent, oldest first, as text", async () => {
    queueDb([campaignRow()], [mirrorRow(INBOUND), mirrorRow(OUTBOUND)]);

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

  it("reads our OWN mirror — a real exchange renders with the provider unreachable", async () => {
    queueDb([campaignRow()], [mirrorRow(OUTBOUND), mirrorRow(INBOUND)]);
    // The plan is cancelled: every live call fails, and the key cannot resolve.
    mockListEmails.mockRejectedValue(new Error("Instantly 404 workspace gone"));
    mockResolveKey.mockRejectedValue(new Error("key-service 404"));

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.source).toBe("mirror");
    expect(conv.messageCount).toBe(2);
    expect(conv.messages[1].text).toBe("Sure — what does it cost?");
  });

  it("spends NO Instantly quota when the mirror holds the thread", async () => {
    queueDb([campaignRow()], [mirrorRow(OUTBOUND)]);

    await fetchLeadConversation(INPUT);

    expect(mockListEmails).not.toHaveBeenCalled();
    expect(mockResolveKey).not.toHaveBeenCalled();
  });

  it("does NOT start at the prospect's reply — our own words are what they answered", async () => {
    queueDb(
      [campaignRow()],
      [
        mirrorRow(email({ id: "s1", ue_type: 1, timestamp_email: "2026-09-01T10:00:00.000Z" })),
        mirrorRow(email({ id: "s2", ue_type: 1, timestamp_email: "2026-09-04T10:00:00.000Z" })),
        mirrorRow(
          email({
            id: "r1",
            ue_type: 2,
            timestamp_email: "2026-09-05T10:00:00.000Z",
            body: { text: "ok" },
          } as Partial<EmailRecord>),
        ),
      ],
    );

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.messages).toHaveLength(3);
    expect(conv.messages[0].direction).toBe("outbound");
  });

  it("scopes the lookup to the caller's org and matches the email case-insensitively", async () => {
    queueDb([campaignRow()], [mirrorRow(OUTBOUND)]);

    await fetchLeadConversation(INPUT);

    const text = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("c.org_id =");
    expect(text).toContain("lower(c.lead_email) = lower(");
  });

  it("returns the STORED lead email, not the caller's spelling", async () => {
    queueDb([campaignRow()], [mirrorRow(OUTBOUND)]);

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.leadEmail).toBe("alice@media.com");
  });
});

describe("fetchLeadConversation — an INCOMPLETE mirror", () => {
  it("asks the provider once when our event log says mail was exchanged, and stores it", async () => {
    // Mirror empty, but silver holds a real send/reply for this sequence.
    queueDb([campaignRow()], [], [{ present: 1 }]);
    mockListEmails.mockResolvedValue([
      email({ id: "e1", ue_type: 1, body: { text: "worth a chat?" } } as Partial<EmailRecord>),
    ]);

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.source).toBe("provider");
    expect(conv.messageCount).toBe(1);
    // What it read is mirrored, so the next read costs nothing.
    expect(mockInsertEmailsBatch).toHaveBeenCalledWith(
      "ic-1",
      "org-1",
      expect.arrayContaining([expect.objectContaining({ id: "e1" })]),
    );
  });

  it("gates that call on real evidence only — the evidence query ignores inferred events", async () => {
    queueDb([campaignRow()], [], [{ present: 1 }]);
    mockListEmails.mockResolvedValue([]);

    await fetchLeadConversation(INPUT);

    const evidenceSql = extractSqlText(mockDbExecute.mock.calls[2][0]);
    expect(evidenceSql).toContain("inferred = false");
    expect(evidenceSql).toContain("instantly_events");
  });

  it("an unreachable provider on an incomplete mirror is a 502, NEVER an empty thread", async () => {
    queueDb([campaignRow()], [], [{ present: 1 }]);
    mockListEmails.mockRejectedValue(new Error("Instantly 503"));

    const err = await fetchLeadConversation(INPUT).catch((e) => e);
    expect(err).toBeInstanceOf(LeadConversationError);
    expect(err.code).toBe("thread_unavailable");
    expect(err.status).toBe(502);
  });

  it("a mirror it cannot even READ is a 502, not an empty thread", async () => {
    mockDbExecute.mockReset();
    mockDbExecute
      .mockResolvedValueOnce(pgResult([campaignRow()]))
      .mockRejectedValueOnce(new Error("connection reset"));

    await expect(fetchLeadConversation(INPUT)).rejects.toMatchObject({
      code: "thread_unavailable",
      status: 502,
    });
  });
});

describe("fetchLeadConversation — self-send transport", () => {
  it("reads the thread out of bronze, in the same shape", async () => {
    queueDb([campaignRow({ instantlyCampaignId: "self:abc", sendTransport: "smtp" })]);
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
    expect(conv.source).toBe("self_send");
    expect(conv.messages.map((m) => m.direction)).toEqual(["outbound", "inbound"]);
    expect(conv.messages[1].text).toBe("Sure — what does it cost?");
  });
});

describe("fetchLeadConversation — refusals", () => {
  it("a conversation nobody has on record is a 404, NOT an empty list", async () => {
    queueDb([]);

    await expect(fetchLeadConversation(INPUT)).rejects.toMatchObject({
      code: "campaign_not_found",
      status: 404,
    });
    expect(mockListEmails).not.toHaveBeenCalled();
  });

  it("a sequence that exists with nothing exchanged is a 200 with an empty list", async () => {
    // Empty mirror AND no real send/reply on record: nothing was exchanged.
    queueDb([campaignRow()], [], []);

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.messageCount).toBe(0);
    expect(conv.messages).toEqual([]);
    // An empty conversation is not a reason to spend Instantly quota.
    expect(mockListEmails).not.toHaveBeenCalled();
  });

  it("a stored thread that cannot be read fails loud too", async () => {
    queueDb([campaignRow({ sendTransport: "smtp" })]);
    mockFetchSelfSendThread.mockRejectedValue(new Error("connection reset"));

    await expect(fetchLeadConversation(INPUT)).rejects.toMatchObject({
      code: "thread_unavailable",
      status: 502,
    });
  });
});

// ─── The whole campaign, not one stored row ──────────────────────────────────

/**
 * Route `db.execute` by the table it names rather than by call order: the
 * sequences' threads are fetched concurrently, so their mirror reads interleave
 * and a positional queue would assert on an order nothing guarantees.
 */
function routeDb(opts: {
  campaigns: Record<string, unknown>[];
  mirror?: Record<string, EmailRecord[]>;
  exchanged?: string[];
}) {
  mockDbExecute.mockReset();
  mockDbExecute.mockImplementation((query: unknown) => {
    const text = extractSqlText(query);
    if (text.includes("instantly_campaigns")) {
      return Promise.resolve(pgResult(opts.campaigns));
    }
    const id = Object.keys(opts.mirror ?? {}).find((k) => text.includes(k));
    if (text.includes("instantly_emails_raw")) {
      return Promise.resolve(
        pgResult((id ? opts.mirror?.[id] ?? [] : []).map((r) => ({ payload: r }))),
      );
    }
    if (text.includes("instantly_events")) {
      const hit = (opts.exchanged ?? []).some((k) => text.includes(k));
      return Promise.resolve(pgResult(hit ? [{ present: 1 }] : []));
    }
    return Promise.resolve(pgResult([]));
  });
}

const OLD_ROW = campaignRow({
  campaignId: "camp-old",
  instantlyCampaignId: "ic-old",
  createdAt: "2026-05-09 11:33:25",
  accountEmail: "lourd@growthagency.forum",
});
const NEW_ROW = campaignRow({
  campaignId: "camp-1",
  createdAt: "2026-06-26 08:17:55",
});

describe("fetchLeadConversation — the whole campaign, not one stored row", () => {
  it("merges every stored row of the campaign into ONE ordered thread", async () => {
    // The measured shape: the first three emails sat under a SIBLING row, so
    // reading the asked row alone placed the May reply above a July send.
    mockGetCampaignFamily.mockResolvedValue(["camp-old", "camp-1"]);
    routeDb({
      campaigns: [OLD_ROW, NEW_ROW],
      mirror: {
        "ic-old": [
          email({ id: "o1", timestamp_email: "2026-05-09T11:33:00.000Z" }),
          email({ id: "o2", timestamp_email: "2026-05-22T11:00:00.000Z" }),
          email({
            id: "o3",
            ue_type: 2,
            timestamp_email: "2026-05-24T09:00:00.000Z",
            body: { text: "interested" },
          } as Partial<EmailRecord>),
        ],
        "ic-1": [
          email({ id: "n1", timestamp_email: "2026-07-01T10:00:00.000Z" }),
          email({ id: "n2", timestamp_email: "2026-07-04T10:00:00.000Z" }),
        ],
      },
    });

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.messageCount).toBe(5);
    expect(conv.messages.map((m) => m.at)).toEqual([
      "2026-05-09T11:33:00.000Z",
      "2026-05-22T11:00:00.000Z",
      "2026-05-24T09:00:00.000Z",
      "2026-07-01T10:00:00.000Z",
      "2026-07-04T10:00:00.000Z",
    ]);
    // The earliest OUTBOUND is the send the delivery evidence reports as first,
    // so a consumer can place the delivery facts against a message on screen.
    expect(conv.messages.find((m) => m.direction === "outbound")?.at).toBe(
      "2026-05-09T11:33:00.000Z",
    );
    // The reply now sits BELOW the email it answered.
    expect(conv.messages.findIndex((m) => m.direction === "inbound")).toBe(2);
    // Every message says which stored row carried it.
    expect(conv.messages[0].campaignId).toBe("camp-old");
    expect(conv.messages[0].instantlyCampaignId).toBe("ic-old");
    expect(conv.messages[4].campaignId).toBe("camp-1");
    expect(conv.campaignIds).toEqual(["camp-old", "camp-1"]);
    expect(conv.sequences.map((s) => s.messageCount)).toEqual([3, 2]);
  });

  it("mixes the two transports in one thread — the consumer cannot know which carried what", async () => {
    mockGetCampaignFamily.mockResolvedValue(["camp-old", "camp-1"]);
    routeDb({
      campaigns: [
        campaignRow({
          campaignId: "camp-old",
          instantlyCampaignId: "self:abc",
          sendTransport: "smtp",
          createdAt: "2026-05-09 11:33:25",
        }),
        NEW_ROW,
      ],
      mirror: {
        "ic-1": [email({ id: "n1", timestamp_email: "2026-07-01T10:00:00.000Z" })],
      },
    });
    mockFetchSelfSendThread.mockResolvedValue([
      {
        direction: "outbound",
        from: "amy@boostdistribute.com",
        to: "alice@media.com",
        date: "2026-05-09T11:33:00.000Z",
        subject: "quick question",
        bodyText: "worth a chat?",
      },
    ]);

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.messageCount).toBe(2);
    expect(conv.messages.map((m) => m.instantlyCampaignId)).toEqual([
      "self:abc",
      "ic-1",
    ]);
    expect(conv.sequences.map((s) => s.transport)).toEqual(["smtp", "instantly"]);
    expect(conv.sequences.map((s) => s.source)).toEqual(["self_send", "mirror"]);
  });

  it("a single-row campaign answers exactly as it did before", async () => {
    routeDb({
      campaigns: [campaignRow()],
      mirror: { "ic-1": [email({ id: "s1" })] },
    });

    const conv = await fetchLeadConversation(INPUT);

    expect(conv.campaignId).toBe("camp-1");
    expect(conv.campaignIds).toEqual(["camp-1"]);
    expect(conv.instantlyCampaignId).toBe("ic-1");
    expect(conv.transport).toBe("instantly");
    expect(conv.source).toBe("mirror");
    expect(conv.accountEmail).toBe("amy@boostdistribute.com");
    expect(conv.messageCount).toBe(1);
  });

  it("asks the DB for the campaign's rows in ONE query, never `= ANY(<js array>)`", async () => {
    mockGetCampaignFamily.mockResolvedValue(["camp-old", "camp-1"]);
    routeDb({ campaigns: [campaignRow()] });

    await fetchLeadConversation(INPUT);

    const lookups = mockDbExecute.mock.calls.filter((c) =>
      extractSqlText(c[0]).includes("instantly_campaigns"),
    );
    expect(lookups).toHaveLength(1);
    const text = extractSqlText(lookups[0][0]);
    expect(text).toContain("c.campaign_id IN (");
    // Never `= ANY(<js array>)` — drizzle expands that into a ROW expression
    // which trips Postgres' 1664-entry limit.
    expect(text).not.toContain("ANY(");
  });
});

describe("fetchLeadConversation — a source that fails is never dropped", () => {
  it("campaign-service unreachable fails loud — it must not degrade to one row", async () => {
    mockGetCampaignFamily.mockRejectedValue(new Error("campaign-service 503"));
    routeDb({ campaigns: [campaignRow()] });

    const err = await fetchLeadConversation(INPUT).catch((e) => e);
    expect(err).toBeInstanceOf(LeadConversationError);
    expect(err.code).toBe("campaign_identity_unavailable");
    expect(err.status).toBe(502);
    // Nothing partial was read, let alone returned.
    expect(mockDbExecute).not.toHaveBeenCalled();
    expect(mockListEmails).not.toHaveBeenCalled();
  });

  it("ONE sibling row failing takes the whole read down — half a thread is worse", async () => {
    mockGetCampaignFamily.mockResolvedValue(["camp-old", "camp-1"]);
    routeDb({
      campaigns: [OLD_ROW, NEW_ROW],
      // The old row's mirror is empty but our own events say it exchanged mail,
      // so it asks the provider — which is gone.
      exchanged: ["ic-old"],
      mirror: { "ic-1": [email({ id: "n1" })] },
    });
    mockListEmails.mockRejectedValue(new Error("Instantly 503"));

    const err = await fetchLeadConversation(INPUT).catch((e) => e);
    expect(err).toBeInstanceOf(LeadConversationError);
    expect(err.code).toBe("thread_unavailable");
    expect(err.status).toBe(502);
  });

  it("refuses rather than truncating when the lead sits in more rows than it fans out to", async () => {
    const many = Array.from({ length: MAX_CONVERSATION_SEQUENCES + 1 }, (_, i) =>
      campaignRow({ campaignId: `camp-${i}`, instantlyCampaignId: `ic-${i}` }),
    );
    mockGetCampaignFamily.mockResolvedValue(many.map((m) => m.campaignId));
    routeDb({ campaigns: many });

    const err = await fetchLeadConversation(INPUT).catch((e) => e);
    expect(err.code).toBe("too_many_sequences");
    expect(err.status).toBe(502);
    expect(mockListEmails).not.toHaveBeenCalled();
  });
});

describe("mergeConversationMessages", () => {
  const seq = (campaignId: string, instantlyCampaignId: string) => ({
    campaignId,
    instantlyCampaignId,
    leadEmail: "alice@media.com",
    accountEmail: null,
    sendTransport: "instantly" as const,
    createdAt: null,
  });
  const msg = (date: string, bodyText = "x") => ({
    direction: "outbound" as const,
    from: "a@b.c",
    to: "d@e.f",
    date,
    subject: "s",
    bodyText,
  });

  it("orders by the message's own timestamp, across sequences", () => {
    const merged = mergeConversationMessages([
      { sequence: seq("c2", "i2"), messages: [msg("2026-07-01T00:00:00.000Z")] },
      { sequence: seq("c1", "i1"), messages: [msg("2026-05-09T00:00:00.000Z")] },
    ]);
    expect(merged.map((m) => m.campaignId)).toEqual(["c1", "c2"]);
  });

  it("keeps an undated message rather than dropping it — we hold it, we cannot place it", () => {
    const merged = mergeConversationMessages([
      { sequence: seq("c1", "i1"), messages: [msg(""), msg("2026-05-09T00:00:00.000Z")] },
    ]);
    expect(merged).toHaveLength(2);
    expect(merged[0].at).toBe("2026-05-09T00:00:00.000Z");
    expect(merged[1].at).toBe("");
  });

  it("breaks a tie on the order the sequences happened in, never on a fabricated time", () => {
    const at = "2026-05-09T00:00:00.000Z";
    const merged = mergeConversationMessages([
      { sequence: seq("c1", "i1"), messages: [msg(at, "first")] },
      { sequence: seq("c2", "i2"), messages: [msg(at, "second")] },
    ]);
    expect(merged.map((m) => m.text)).toEqual(["first", "second"]);
  });
});
