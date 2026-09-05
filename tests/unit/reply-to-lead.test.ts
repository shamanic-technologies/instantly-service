import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbExecute = vi.fn();
const mockInsertValues = vi.fn();
vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockDbExecute(...a),
    insert: () => ({ values: (...a: unknown[]) => mockInsertValues(...a) }),
  },
}));
vi.mock("../../src/db/schema", () => ({
  smtpDispatchRaw: { id: "id" },
  scheduledReplies: { id: "id", scheduledFor: "scheduled_for" },
}));

const mockListEmails = vi.fn();
const mockGetAccount = vi.fn();
const mockReplyToEmail = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listEmails: (...a: unknown[]) => mockListEmails(...a),
  getAccount: (...a: unknown[]) => mockGetAccount(...a),
  replyToEmail: (...a: unknown[]) => mockReplyToEmail(...a),
}));

const mockResolveKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...a: unknown[]) => mockResolveKey(...a),
}));

const mockResolveCredential = vi.fn();
vi.mock("../../src/lib/self-send/mailbox-credentials", () => ({
  resolveMailboxCredential: (...a: unknown[]) => mockResolveCredential(...a),
}));

const mockDispatchMessage = vi.fn();
vi.mock("../../src/lib/self-send/smtp", () => ({
  dispatchMessage: (...a: unknown[]) => mockDispatchMessage(...a),
}));

import {
  MANUAL_REPLY_STEP,
  replySubject,
  replyToLead,
  ReplyToLeadError,
  selectReplyTarget,
} from "../../src/lib/reply-to-lead";
import { buildReplyBodyWithSignature, UNSUBSCRIBE_FOOTER_HTML } from "../../src/lib/send-lead";
import type { Account, EmailRecord } from "../../src/lib/instantly-client";

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

const ACCOUNT: Account = {
  email: "amy@boostdistribute.com",
  warmup_status: 1,
  status: 1,
  first_name: "Amy",
  last_name: "Moore",
};

function email(over: Partial<EmailRecord>): EmailRecord {
  return {
    id: "e1",
    campaign_id: "ic-1",
    lead: "alice@media.com",
    lead_id: null,
    eaccount: "amy@boostdistribute.com",
    ue_type: 2,
    step: "1",
    timestamp_email: "2026-09-01T10:00:00.000Z",
    ...over,
  } as EmailRecord;
}

const INPUT = {
  orgId: "org-1",
  userId: "user-1",
  campaignId: "camp-1",
  leadEmail: "Alice@Media.com",
  bodyHtml: "<p>Thursday works.</p>",
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

/**
 * A Wednesday 09:00 in `America/Chicago` — inside the fleet default window, so
 * a reply sent at this instant goes out immediately. Pinned rather than `new
 * Date()`: the endpoint now WAITS outside the prospect's business hours, so a
 * test on real time would pass or schedule depending on the hour it ran at.
 */
const IN_WINDOW = new Date("2026-09-02T14:00:00.000Z");

/** A Wednesday 23:00 local in the same zone — the window is closed. */
const OUT_OF_WINDOW = new Date("2026-09-03T04:00:00.000Z");

/** Reply at an instant the prospect can be mailed, and assert it went out. */
async function sendNow(input = INPUT) {
  const outcome = await replyToLead(input, { asOf: IN_WINDOW });
  if (outcome.status !== "sent") {
    throw new Error(`expected the reply to be sent, got "${outcome.status}"`);
  }
  return outcome.reply;
}

beforeEach(() => {
  vi.resetAllMocks();
  mockResolveKey.mockResolvedValue({ key: "k", keySource: "platform" });
  mockGetAccount.mockResolvedValue(ACCOUNT);
  mockInsertValues.mockResolvedValue(undefined);
});

// ─── Pure ────────────────────────────────────────────────────────────────────

describe("selectReplyTarget", () => {
  it("threads onto the prospect's LATEST inbound message", () => {
    const target = selectReplyTarget([
      email({ id: "old", timestamp_email: "2026-08-01T10:00:00.000Z" }),
      email({ id: "new", timestamp_email: "2026-09-01T10:00:00.000Z" }),
    ]);
    expect(target?.emailId).toBe("new");
  });

  it("never threads onto one of OUR OWN messages", () => {
    // ue_type 1 = sent by us, 3 = manual-sent, 4 = scheduled. Replying to our
    // own email would open a second branch of the conversation.
    expect(
      selectReplyTarget([
        email({ id: "sent", ue_type: 1 }),
        email({ id: "manual", ue_type: 3 }),
        email({ id: "scheduled", ue_type: 4 }),
      ]),
    ).toBeNull();
  });

  it("is null when the lead never wrote back", () => {
    expect(selectReplyTarget([])).toBeNull();
  });
});

describe("replySubject", () => {
  it("prefixes Re: once and never doubles an existing one", () => {
    expect(replySubject("Quick question")).toBe("Re: Quick question");
    expect(replySubject("Re: Quick question")).toBe("Re: Quick question");
  });

  it("stays empty rather than becoming a bare Re:", () => {
    expect(replySubject("   ")).toBe("");
  });
});

describe("buildReplyBodyWithSignature", () => {
  it("signs with the account's OWN persona", () => {
    const html = buildReplyBodyWithSignature("<p>Hi</p>", ACCOUNT);
    expect(html).toContain("Amy Moore");
    expect(html).toContain("<p>--</p>");
  });

  it("does NOT append the unsubscribe footer — a threaded reply is not bulk mail", () => {
    const html = buildReplyBodyWithSignature("<p>Hi</p>", ACCOUNT);
    // `{unsubscribe_link}` is Instantly's SERVER-SIDE merge variable and only
    // resolves on a campaign send; on a reply it would ship as a dead link.
    expect(html).not.toContain("{unsubscribe_link}");
    expect(html).not.toContain(UNSUBSCRIBE_FOOTER_HTML);
  });

  it("is idempotent — a re-signed body never stacks signatures", () => {
    const once = buildReplyBodyWithSignature("<p>Hi</p>", ACCOUNT);
    expect(buildReplyBodyWithSignature(once, ACCOUNT)).toBe(once);
  });
});

// ─── Instantly transport ─────────────────────────────────────────────────────

describe("replyToLead over Instantly", () => {
  it("replies into the existing thread from the mailbox that contacted the lead", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([
      email({ id: "inbound-1", subject: "Quick question" }),
    ]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    const result = await sendNow();

    expect(mockReplyToEmail).toHaveBeenCalledWith("k", {
      eaccount: "amy@boostdistribute.com",
      replyToUuid: "inbound-1",
      subject: "Re: Quick question",
      bodyHtml: expect.stringContaining("Amy Moore"),
      ccAddressEmailList: "kevin@distribute.you",
    });
    expect(result.transport).toBe("instantly");
    expect(result.accountEmail).toBe("amy@boostdistribute.com");
    expect(result.from).toBe('"Amy Moore" <amy@boostdistribute.com>');
    expect(result.inReplyTo).toBe("inbound-1");
    expect(result.messageId).toBe("sent-1");
  });

  it("falls back to the mailbox on the inbound record when the row predates account_email", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow({ accountEmail: null })]));
    mockListEmails.mockResolvedValue([
      email({ id: "inbound-1", eaccount: "louis@maildistribute.com" }),
    ]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    const result = await sendNow();
    expect(result.accountEmail).toBe("louis@maildistribute.com");
  });

  it("refuses with no_reply_to_thread rather than sending a fresh email", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "ours", ue_type: 1 })]);

    await expect(sendNow()).rejects.toMatchObject({
      code: "no_reply_to_thread",
      status: 409,
    });
    expect(mockReplyToEmail).not.toHaveBeenCalled();
  });

  it("refuses when no mailbox can be attributed", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow({ accountEmail: null })]));
    mockListEmails.mockResolvedValue([email({ id: "inbound-1", eaccount: "" })]);

    await expect(sendNow()).rejects.toMatchObject({
      code: "sending_account_unresolved",
      status: 409,
    });
    expect(mockReplyToEmail).not.toHaveBeenCalled();
  });

  it("surfaces a transport refusal as reply_dispatch_failed, never as success", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "inbound-1" })]);
    mockReplyToEmail.mockRejectedValue(new Error("Instantly 402"));

    await expect(sendNow()).rejects.toMatchObject({
      code: "reply_dispatch_failed",
      status: 502,
    });
  });

  it("records the reply in bronze at step 0, like the self-send branch does", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([
      email({ id: "inbound-1", subject: "Quick question" }),
    ]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    await sendNow();

    // Instantly holds this reply too — but only until the plan is cancelled,
    // which permanently deletes every conversation those mailboxes carried.
    expect(mockInsertValues).toHaveBeenCalledTimes(1);
    const row = mockInsertValues.mock.calls[0][0];
    expect(row).toMatchObject({
      instantlyCampaignId: "ic-1",
      leadEmail: "alice@media.com",
      accountEmail: "amy@boostdistribute.com",
      step: MANUAL_REPLY_STEP,
      outcome: "sent",
    });
    expect(row.payload).toMatchObject({
      kind: "manual_reply",
      transport: "instantly",
      subject: "Re: Quick question",
      inReplyTo: "inbound-1",
      instantlyEmailId: "sent-1",
    });
    expect(row.payload.bodyHtml).toContain("Amy Moore");
  });

  it("stores NO wire Message-Id, because Instantly does not return one", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "inbound-1" })]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    await sendNow();

    // `sent-1` is Instantly's own email UUID; it appears in no mail header, so
    // it could never match an In-Reply-To / References on the prospect's next
    // answer. Putting it in message_id would plant a correlation key that looks
    // usable and silently never fires — the anchor query filters
    // `message_id IS NOT NULL`, so null keeps this row honestly out of it.
    const row = mockInsertValues.mock.calls[0][0];
    expect(row.messageId).toBeNull();
    expect(row.payload.instantlyEmailId).toBe("sent-1");
  });

  it("records a refused reply too, so it is not invisible", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "inbound-1" })]);
    mockReplyToEmail.mockRejectedValue(new Error("Instantly 402"));

    await expect(sendNow()).rejects.toMatchObject({
      code: "reply_dispatch_failed",
    });

    const row = mockInsertValues.mock.calls[0][0];
    expect(row).toMatchObject({ outcome: "transient", step: MANUAL_REPLY_STEP });
    expect(row.payload.error).toContain("Instantly 402");
    // Nothing left the building, so there is no body to keep.
    expect(row.payload.bodyHtml).toBeUndefined();
  });

  it("does not lose the dispatch error to a bookkeeping failure", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "inbound-1" })]);
    mockReplyToEmail.mockRejectedValue(new Error("Instantly 402"));
    mockInsertValues.mockRejectedValueOnce(new Error("db down"));

    // The caller must still learn why the reply did not go out.
    await expect(sendNow()).rejects.toMatchObject({
      code: "reply_dispatch_failed",
      status: 502,
    });
  });
});

describe("replyToLead lookup", () => {
  it("404s campaign_not_found when this org holds no campaign for the lead", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    await expect(sendNow()).rejects.toMatchObject({
      code: "campaign_not_found",
      status: 404,
    });
  });

  it("matches the lead case-insensitively", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "inbound-1" })]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    await sendNow();
    const sqlText = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(sqlText.toLowerCase()).toContain("lower(c.lead_email)");
  });

  it("is a ReplyToLeadError so a caller can branch on the code", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    await expect(sendNow()).rejects.toBeInstanceOf(ReplyToLeadError);
  });
});

// ─── Self-send transport ─────────────────────────────────────────────────────

describe("replyToLead over the self-send transport", () => {
  const CREDENTIAL = {
    address: "amy@boostdistribute.com",
    appPassword: "pw",
    smtpHost: "smtp.gmail.com",
    imapHost: "imap.gmail.com",
  };

  function primeSelfSend() {
    mockDbExecute
      // campaign row
      .mockResolvedValueOnce(
        pgResult([campaignRow({ instantlyCampaignId: "self:abc", sendTransport: "smtp" })]),
      )
      // latest inbound
      .mockResolvedValueOnce(
        pgResult([{ messageId: "<their-2@mail>", subject: "Quick question", at: "2026-09-01" }]),
      )
      // the whole conversation, oldest first
      .mockResolvedValueOnce(
        pgResult([
          { messageId: "<ours-1@mail>" },
          { messageId: "<their-2@mail>" },
        ]),
      )
      // the sending account persona
      .mockResolvedValueOnce(pgResult([{ email: ACCOUNT.email, firstName: "Amy", lastName: "Moore" }]));
  }

  it("sends the reply itself, threaded on the prospect's own Message-Id", async () => {
    primeSelfSend();
    mockResolveCredential.mockResolvedValue(CREDENTIAL);
    mockDispatchMessage.mockResolvedValue({
      messageId: "<ours-3@mail>",
      response: "250 OK",
      accepted: ["alice@media.com"],
      rejected: [],
    });

    const result = await sendNow();

    const message = mockDispatchMessage.mock.calls[0][1];
    expect(message.inReplyTo).toBe("<their-2@mail>");
    expect(message.references).toEqual(["<ours-1@mail>", "<their-2@mail>"]);
    expect(message.from).toBe('"Amy Moore" <amy@boostdistribute.com>');
    expect(message.subject).toBe("Re: Quick question");
    // A one-to-one answer carries no bulk-mail unsubscribe header.
    expect(message.headers).toEqual({});
    expect(result.transport).toBe("smtp");
    expect(result.messageId).toBe("<ours-3@mail>");
  });

  it("records the reply in bronze at step 0, outside the sequence", async () => {
    primeSelfSend();
    mockResolveCredential.mockResolvedValue(CREDENTIAL);
    mockDispatchMessage.mockResolvedValue({
      messageId: "<ours-3@mail>",
      response: "250 OK",
      accepted: ["alice@media.com"],
      rejected: [],
    });

    await sendNow();

    expect(MANUAL_REPLY_STEP).toBe(0);
    const row = mockInsertValues.mock.calls[0][0];
    expect(row).toMatchObject({
      instantlyCampaignId: "self:abc",
      accountEmail: "amy@boostdistribute.com",
      step: 0,
      outcome: "sent",
      messageId: "<ours-3@mail>",
    });
    expect(row.payload.kind).toBe("manual_reply");
  });

  it("refuses when we hold no credential for the mailbox that contacted the lead", async () => {
    primeSelfSend();
    mockResolveCredential.mockRejectedValue(new Error("no mailbox"));

    await expect(sendNow()).rejects.toMatchObject({
      code: "mailbox_credential_unavailable",
      status: 409,
    });
    expect(mockDispatchMessage).not.toHaveBeenCalled();
  });

  it("refuses with no_reply_to_thread when nothing came back on this sequence", async () => {
    mockDbExecute
      .mockResolvedValueOnce(
        pgResult([campaignRow({ instantlyCampaignId: "self:abc", sendTransport: "smtp" })]),
      )
      .mockResolvedValueOnce(pgResult([]));

    await expect(sendNow()).rejects.toMatchObject({
      code: "no_reply_to_thread",
      status: 409,
    });
    expect(mockDispatchMessage).not.toHaveBeenCalled();
  });

  it("records a refused attempt and reports it, never a silent success", async () => {
    primeSelfSend();
    mockResolveCredential.mockResolvedValue(CREDENTIAL);
    mockDispatchMessage.mockRejectedValue(new Error("550 blocked"));

    await expect(sendNow()).rejects.toMatchObject({
      code: "reply_dispatch_failed",
      status: 502,
    });
    expect(mockInsertValues.mock.calls[0][0]).toMatchObject({ outcome: "transient", step: 0 });
  });
});


// ─── Agency inbox in CC ──────────────────────────────────────────────────────

/**
 * A human has to be able to read the exchange AND be pulled into it. Only a
 * VISIBLE CC survives a reply-all — on a BCC the prospect's next answer never
 * reaches the agency inbox, so the thread silently goes dark again the moment
 * the conversation continues. Both transports carry it, because an inbox cannot
 * tell which pipe a given reply happened to take.
 */
describe("the agency inbox rides every reply, in CC", () => {
  it("CCs it on the Instantly transport, and never BCCs", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "inbound-1", subject: "Hi" })]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    const result = await sendNow();

    const body = mockReplyToEmail.mock.calls[0][1] as Record<string, unknown>;
    // Instantly's contract for this field is a COMMA-SEPARATED STRING, not an
    // array — an array is not silently coerced into one.
    expect(body.ccAddressEmailList).toBe("kevin@distribute.you");
    expect(typeof body.ccAddressEmailList).toBe("string");
    expect(Object.keys(body)).not.toContain("bccAddressEmailList");
    expect(result.cc).toBe("kevin@distribute.you");
  });

  it("CCs it on the self-send transport, and never BCCs", async () => {
    mockDbExecute
      .mockResolvedValueOnce(pgResult([campaignRow({ sendTransport: "smtp" })]))
      .mockResolvedValueOnce(
        pgResult([{ messageId: "<in-1@media.com>", subject: "Hi", at: "2026-09-01" }]),
      )
      .mockResolvedValueOnce(pgResult([{ messageId: "<in-1@media.com>" }]))
      .mockResolvedValueOnce(pgResult([{ firstName: "Amy", lastName: "Moore" }]));
    mockResolveCredential.mockResolvedValue({
      address: "amy@boostdistribute.com",
      appPassword: "pw",
      smtpHost: "smtp.gmail.com",
      imapHost: "imap.gmail.com",
    });
    mockDispatchMessage.mockResolvedValue({
      messageId: "<out-1@boostdistribute.com>",
      response: "250 OK",
      accepted: ["alice@media.com"],
      rejected: [],
    });

    const result = await sendNow();

    const message = mockDispatchMessage.mock.calls[0][1] as Record<string, unknown>;
    expect(message.cc).toBe("kevin@distribute.you");
    expect(Object.keys(message)).not.toContain("bcc");
    expect(message.headers).not.toHaveProperty("Bcc");
    expect(result.cc).toBe("kevin@distribute.you");
  });

  it("follows ADMIN_NOTIFICATION_EMAIL — the address has ONE home, read at use", async () => {
    const previous = process.env.ADMIN_NOTIFICATION_EMAIL;
    process.env.ADMIN_NOTIFICATION_EMAIL = "inbox@agency.test";
    try {
      mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
      mockListEmails.mockResolvedValue([email({ id: "inbound-1", subject: "Hi" })]);
      mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

      const result = await sendNow();

      expect(result.cc).toBe("inbox@agency.test");
    } finally {
      if (previous === undefined) delete process.env.ADMIN_NOTIFICATION_EMAIL;
      else process.env.ADMIN_NOTIFICATION_EMAIL = previous;
    }
  });

  it("records the CC in bronze, so who was on the message stays recoverable", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "inbound-1", subject: "Hi" })]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    await sendNow();

    const row = mockInsertValues.mock.calls[0][0] as {
      step: number;
      payload: Record<string, unknown>;
    };
    expect(row.step).toBe(MANUAL_REPLY_STEP);
    expect(row.payload.cc).toBe("kevin@distribute.you");
  });
});

// ─── The answer waits for the prospect's own business hours ──────────────────

describe("a reply waits for the prospect's sending window", () => {
  it("sends immediately when their window is open", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "in-1", subject: "Quick question" })]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    const outcome = await replyToLead(INPUT, { asOf: IN_WINDOW });

    expect(outcome.status).toBe("sent");
    expect(mockReplyToEmail).toHaveBeenCalledTimes(1);
  });

  it("does NOT send at 23:00 in the prospect's day — it schedules the next opening", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "in-1", subject: "Quick question" })]);
    // The insert is the enqueue; it must return the stored row.
    mockInsertValues.mockReturnValue({
      returning: () =>
        Promise.resolve([{ id: "sr-1", scheduledFor: new Date("2026-09-03T13:00:00.000Z") }]),
    });

    const outcome = await replyToLead(INPUT, { asOf: OUT_OF_WINDOW });

    expect(outcome.status).toBe("scheduled");
    // Nothing left the building.
    expect(mockReplyToEmail).not.toHaveBeenCalled();
    if (outcome.status !== "scheduled") throw new Error("unreachable");
    // 08:00 America/Chicago the next morning = 13:00 UTC.
    expect(outcome.scheduled.scheduledFor).toBe("2026-09-03T13:00:00.000Z");
    expect(outcome.scheduled.timezone).toBe("America/Chicago");
    // Resolved NOW, not deferred to dispatch — the caller learns who answers.
    expect(outcome.scheduled.accountEmail).toBe("amy@boostdistribute.com");
  });

  it("resolves the window in the LEAD's timezone, not ours", async () => {
    // 04:00 UTC is 23:00 in Chicago (closed) but 13:00 in Tokyo (open).
    mockDbExecute.mockResolvedValueOnce(
      pgResult([campaignRow({ timezone: "Asia/Tokyo" })]),
    );
    mockListEmails.mockResolvedValue([email({ id: "in-1", subject: "Quick question" })]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    const outcome = await replyToLead(INPUT, { asOf: OUT_OF_WINDOW });

    expect(outcome.status).toBe("sent");
  });

  it("raises the named refusals SYNCHRONOUSLY even when it would wait", async () => {
    // No inbound message: the caller must learn now, not hours later.
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "ours", ue_type: 1 })]);

    await expect(replyToLead(INPUT, { asOf: OUT_OF_WINDOW })).rejects.toMatchObject({
      code: "no_reply_to_thread",
    });
    expect(mockInsertValues).not.toHaveBeenCalled();
  });

  it("a scheduled reply takes NO hold and NO sequence step — only the queue row", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "in-1", subject: "Quick question" })]);
    mockInsertValues.mockReturnValue({
      returning: () => Promise.resolve([{ id: "sr-1", scheduledFor: OUT_OF_WINDOW }]),
    });

    await replyToLead(INPUT, { asOf: OUT_OF_WINDOW });

    // Exactly one insert: the waiting row. No sequence_costs, no sequence_steps,
    // and no bronze dispatch row (nothing was dispatched).
    expect(mockInsertValues).toHaveBeenCalledTimes(1);
    const values = mockInsertValues.mock.calls[0]![0] as Record<string, unknown>;
    expect(values).not.toHaveProperty("step");
    expect(values).not.toHaveProperty("costId");
    expect(values.bodyHtml).toBe(INPUT.bodyHtml);
  });

  it("the drain does NOT re-defer what it already selected", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "in-1", subject: "Quick question" })]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    const outcome = await replyToLead(INPUT, {
      asOf: OUT_OF_WINDOW,
      deferOutsideWindow: false,
    });

    expect(outcome.status).toBe("sent");
  });

  it("still records the reply in bronze at step 0 when it finally goes out", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([campaignRow()]));
    mockListEmails.mockResolvedValue([email({ id: "in-1", subject: "Quick question" })]);
    mockReplyToEmail.mockResolvedValue({ id: "sent-1" });

    await replyToLead(INPUT, { asOf: OUT_OF_WINDOW, deferOutsideWindow: false });

    const values = mockInsertValues.mock.calls.at(-1)![0] as Record<string, unknown>;
    expect(values.step).toBe(MANUAL_REPLY_STEP);
    expect(MANUAL_REPLY_STEP).toBe(0);
  });
});
