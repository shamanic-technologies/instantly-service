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
const mockWroteElsewhere = vi.fn();
vi.mock("../../src/lib/reply-destination", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  findProspectWroteElsewhere: (...a: unknown[]) => mockWroteElsewhere(...a),
}));
vi.mock("../../src/lib/self-send/smtp", () => ({
  dispatchMessage: (...a: unknown[]) => mockDispatchMessage(...a),
}));

// QUOTE-THREAD-1103: every reply without its own quote reads the lead's
// conversation. Mocked here so the mechanics tests keep their queued db reads.
const mockLoadQuoted = vi.fn();
vi.mock("../../src/lib/quoted-conversation", () => ({
  loadQuotedConversation: (...a: unknown[]) => mockLoadQuoted(...a),
}));

// DUALIST-WA-SILENCE-EMAIL: crm-service's "stated not to the brand" read. Only
// asked when the thread carries a brand (most fixtures here carry none).
const mockOptOuts = vi.fn();
vi.mock("../../src/lib/lead-optouts", () => ({
  findStandingOptOuts: (...a: unknown[]) => mockOptOuts(...a),
}));

const mockReadStated = vi.fn();
vi.mock("../../src/lib/crm-client", () => ({
  readStatedNotToBrand: (...a: unknown[]) => mockReadStated(...a) ?? Promise.resolve(new Map()),
}));

import { replyToLead, ReplyToLeadError } from "../../src/lib/reply-to-lead";

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

const THREAD = "self:fd6b0eb8-bed3-4a0d-80f0-5b83f9633578";
const OUR_FIRST = "<5ac53340@marketingagency.group>";
const OUR_ANSWER = "<043d15a1@marketingagency.group>";
const MARKUS_MSG = "<CAE-markus@mail.gmail.com>";

/** The row findThreadMessage reads: Thomas's sequence + where Markus's message sits. */
function chosenRow(over: Record<string, unknown> = {}) {
  return {
    campaignId: "camp-lv",
    instantlyCampaignId: THREAD,
    leadEmail: "thomas@markthalle-luzern.ch",
    accountEmail: "kevin@marketingagency.group",
    sendTransport: "smtp",
    timezone: null,
    brandId: null,
    brandIds: [],
    signatureHtml: null,
    createdAt: "2026-11-01T00:00:00Z",
    mailbox: "kevin@marketingagency.group",
    msgSubject: "Re: Flohsamenschalen für die Markthalle",
    msgAt: new Date("2026-11-09T15:28:51Z"),
    ...over,
  };
}

const CREDENTIAL = {
  address: "kevin@marketingagency.group",
  appPassword: "pw",
  smtpHost: "smtp.gmail.com",
  imapHost: "imap.gmail.com",
};

const ANSWER_TO_MARKUS = {
  orgId: "org-lv",
  userId: "user-1",
  campaignId: "",
  leadEmail: "markus@markthalle-luzern.ch",
  bodyHtml: "<p>Ja, die Flohsamenschalen kommen aus der Schweiz.</p>",
  sentBy: "human" as const,
  threadMessageId: MARKUS_MSG,
  recipients: {
    to: ["markus@markthalle-luzern.ch"],
    cc: ["thomas@markthalle-luzern.ch"],
    bcc: ["kevin@distribute.you"],
  },
};

beforeEach(() => {
  vi.resetAllMocks();
  mockWroteElsewhere.mockResolvedValue(null);
  mockInsertValues.mockResolvedValue(undefined);
  mockLoadQuoted.mockResolvedValue("");
  mockResolveCredential.mockResolvedValue(CREDENTIAL);
  mockOptOuts.mockResolvedValue(new Map());
  mockDispatchMessage.mockResolvedValue({
    messageId: "<ours-new@marketingagency.group>",
    response: "250 OK",
    accepted: ["markus@markthalle-luzern.ch", "thomas@markthalle-luzern.ch"],
    rejected: [],
    rawMessage: Buffer.from(""),
  });
});

function primeChosenThread(over: Record<string, unknown> = {}) {
  mockDbExecute
    // findThreadMessage
    .mockResolvedValueOnce(pgResult([chosenRow(over)]))
    // the sending account persona
    .mockResolvedValueOnce(pgResult([{ email: "kevin@marketingagency.group", firstName: "Kevin", lastName: "Lourd" }]))
    // the thread up to the chosen message, oldest first
    .mockResolvedValueOnce(
      pgResult([{ messageId: OUR_FIRST }, { messageId: MARKUS_MSG }, { messageId: OUR_ANSWER }]),
    );
}

describe("an answer goes to whoever the caller chose, in the thread it chose (EVERY-PERSON-ON-THE-THREAD-1109)", () => {
  it("answers Markus in Thomas's thread: To Markus, Cc Thomas, Bcc as chosen, threaded on HIS message", async () => {
    primeChosenThread();
    const outcome = await replyToLead(ANSWER_TO_MARKUS);

    const message = mockDispatchMessage.mock.calls[0][1];
    expect(message.to).toBe("markus@markthalle-luzern.ch");
    expect(message.cc).toBe("thomas@markthalle-luzern.ch");
    expect(message.bcc).toBe("kevin@distribute.you");
    expect(message.inReplyTo).toBe(MARKUS_MSG);
    // The thread up to the chosen message, the chosen one LAST.
    expect(message.references).toEqual([OUR_FIRST, OUR_ANSWER, MARKUS_MSG]);
    expect(message.subject).toBe("Re: Flohsamenschalen für die Markthalle");
    // Leaves from the mailbox that holds the chosen message.
    expect(mockDispatchMessage.mock.calls[0][0]).toBe(CREDENTIAL);
    expect(outcome.reply.to).toBe("markus@markthalle-luzern.ch");
    expect(outcome.reply.inReplyTo).toBe(MARKUS_MSG);
    // No address-based thread lookup: the message named the thread.
    expect(mockWroteElsewhere).not.toHaveBeenCalled();
  });

  it("records every address on the sent fact, on the thread the message belongs to", async () => {
    primeChosenThread();
    await replyToLead(ANSWER_TO_MARKUS);
    const row = mockInsertValues.mock.calls[0][0];
    expect(row.instantlyCampaignId).toBe(THREAD);
    expect(row.step).toBe(0);
    expect(row.payload).toMatchObject({
      to: "markus@markthalle-luzern.ch",
      cc: "thomas@markthalle-luzern.ch",
      bcc: "kevin@distribute.you",
      inReplyTo: MARKUS_MSG,
      references: [OUR_FIRST, OUR_ANSWER, MARKUS_MSG],
    });
  });

  it("a list left out keeps today's default (Cc = agency inbox), To given", async () => {
    primeChosenThread();
    await replyToLead({ ...ANSWER_TO_MARKUS, recipients: { to: ["markus@markthalle-luzern.ch"] } });
    const message = mockDispatchMessage.mock.calls[0][1];
    expect(message.to).toBe("markus@markthalle-luzern.ch");
    expect(message.cc).toBe("kevin@distribute.you");
    expect(message.bcc).toBeUndefined();
  });

  it("a message this org does not hold = 404 thread_message_not_found, nothing sent", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    const err = await replyToLead(ANSWER_TO_MARKUS).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ReplyToLeadError);
    expect((err as ReplyToLeadError).code).toBe("thread_message_not_found");
    expect((err as ReplyToLeadError).status).toBe(404);
    expect(mockDispatchMessage).not.toHaveBeenCalled();
  });

  it("an automated answer never reaches a chosen recipient who opted out (409, nothing sent)", async () => {
    mockDbExecute
      .mockResolvedValueOnce(pgResult([chosenRow()]))
      // no escalation claimed on the thread
      .mockResolvedValueOnce(pgResult([]));
    mockOptOuts.mockResolvedValue(new Map([["markus@markthalle-luzern.ch", { id: "o1" }]]));
    const err = await replyToLead({ ...ANSWER_TO_MARKUS, sentBy: "automation" }).catch((e: unknown) => e);
    expect((err as ReplyToLeadError).code).toBe("recipient_opted_out");
    expect(mockDispatchMessage).not.toHaveBeenCalled();
  });

  it("a new thread: no In-Reply-To, no References, the subject as given, no quote", async () => {
    mockDbExecute
      // loadCampaign
      .mockResolvedValueOnce(pgResult([chosenRow()]))
      // persona
      .mockResolvedValueOnce(pgResult([{ email: "kevin@marketingagency.group", firstName: "Kevin", lastName: "Lourd" }]));
    const outcome = await replyToLead({
      ...ANSWER_TO_MARKUS,
      campaignId: "camp-lv",
      leadEmail: "thomas@markthalle-luzern.ch",
      threadMessageId: undefined,
      newThreadSubject: "Flohsamenschalen: Muster für die Markthalle",
      recipients: { to: ["thomas@markthalle-luzern.ch"] },
    });
    const message = mockDispatchMessage.mock.calls[0][1];
    expect(message.inReplyTo).toBeUndefined();
    expect(message.references).toBeUndefined();
    expect(message.subject).toBe("Flohsamenschalen: Muster für die Markthalle");
    expect(mockLoadQuoted).not.toHaveBeenCalled();
    expect(outcome.reply.inReplyTo).toBeNull();
    expect(mockInsertValues.mock.calls[0][0].payload.newThread).toBe(true);
  });

  it("chosen recipients on an Instantly-pipe thread are refused 422, nothing sent", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([chosenRow({ sendTransport: "instantly" })]));
    const err = await replyToLead({
      ...ANSWER_TO_MARKUS,
      campaignId: "camp-lv",
      leadEmail: "thomas@markthalle-luzern.ch",
      threadMessageId: undefined,
    }).catch((e: unknown) => e);
    expect((err as ReplyToLeadError).code).toBe("recipients_unsupported_on_transport");
    expect(mockDispatchMessage).not.toHaveBeenCalled();
  });
});
