import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockInsertValues = vi.fn();
const mockPromoteEvent = vi.fn();
const mockDispatchMessage = vi.fn();
const mockResolveCredential = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
    insert: () => ({
      values: (row: unknown) => ({
        returning: () => Promise.resolve(mockInsertValues(row)),
      }),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({ smtpDispatchRaw: { id: "id" } }));

vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...args: unknown[]) => mockPromoteEvent(...args),
}));

vi.mock("../../src/lib/self-send/mailbox-credentials", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  resolveMailboxCredential: (...args: unknown[]) => mockResolveCredential(...args),
}));

vi.mock("../../src/lib/self-send/smtp", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  dispatchMessage: (...args: unknown[]) => mockDispatchMessage(...args),
}));

process.env.SELF_SEND_UNSUBSCRIBE_SECRET = "worker-test-secret";
process.env.SELF_SEND_PUBLIC_URL = "https://opt.test";

const { runDispatch } = await import("../../src/lib/self-send/dispatch-worker");
const { SmtpDispatchError } = await import("../../src/lib/self-send/smtp");

// A MONDAY. The earlier fixture was a Sunday, so these tests were exercising a
// run that the sending calendar now correctly refuses to make at all.
const NOW = new Date("2026-08-17T12:00:00Z");

/**
 * The worker issues three reads in order: pending sequences, sending accounts,
 * then step content per due step. Queueing them keeps the test honest about that
 * ordering instead of matching on SQL text.
 */
function primeReads(options: { hasBody?: boolean } = {}) {
  const { hasBody = true } = options;

  mockExecute
    .mockResolvedValueOnce({
      rows: [
        {
          instantlyCampaignId: "camp-1",
          leadEmail: "prospect@example.com",
          accountEmail: "amy@saviolabsco.com",
          provisionedSteps: [1, 2],
          lastSentStep: null,
          lastSentAt: null,
          stepDelays: [2, null],
        },
      ],
    })
    .mockResolvedValueOnce({
      rows: [
        {
          accountEmail: "amy@saviolabsco.com",
          firstName: "Amy",
          lastName: "Moore",
          dailyLimit: 45,
          timestampCreated: "2020-01-01T00:00:00Z",
          sentToday: 0,
        },
      ],
    })
    .mockResolvedValueOnce({
      rows: [
        {
          bodyHtml: hasBody ? "<p>Hi there</p>" : "",
          subject: "Quick question",
          priorMessageIds: [],
        },
      ],
    });
}

beforeEach(() => {
  vi.resetAllMocks();
  mockInsertValues.mockReturnValue([{ id: "bronze-1" }]);
  mockPromoteEvent.mockResolvedValue({ promoted: true, silverEventId: "ev-1" });
  mockResolveCredential.mockResolvedValue({
    address: "amy@saviolabsco.com",
    appPassword: "pw",
    smtpHost: "smtp.gmail.com",
    imapHost: "imap.gmail.com",
  });
});

describe("runDispatch — success", () => {
  it("sends, records bronze, and promotes a real email_sent", async () => {
    primeReads();
    mockDispatchMessage.mockResolvedValue({
      messageId: "<m1@mail>",
      response: "250 OK",
      accepted: ["prospect@example.com"],
      rejected: [],
    });

    const summary = await runDispatch({ asOf: NOW });

    expect(summary).toMatchObject({ due: 1, sent: 1, bounced: 0, senderBlocked: 0 });

    const bronze = mockInsertValues.mock.calls[0]![0] as Record<string, unknown>;
    expect(bronze.outcome).toBe("sent");
    expect(bronze.messageId).toBe("<m1@mail>");

    const event = mockPromoteEvent.mock.calls[0]![0] as Record<string, unknown>;
    expect(event.eventType).toBe("email_sent");
    expect(event.step).toBe(1);
    expect(event.source).toBe("self_send");
    // Real, not inferred — this is what actualizes the hold.
    expect(event.inferred).toBeUndefined();
    expect(event.sourceRowId).toBe("bronze-1");
  });

  it("signs with the account's own name, not a fallback", async () => {
    primeReads();
    mockDispatchMessage.mockResolvedValue({
      messageId: "<m1@mail>",
      response: "250 OK",
      accepted: ["prospect@example.com"],
      rejected: [],
    });

    await runDispatch({ asOf: NOW });

    const message = mockDispatchMessage.mock.calls[0]![1] as Record<string, unknown>;
    expect(message.from).toBe('"Amy Moore" <amy@saviolabsco.com>');
    expect(message.html).toContain("Amy Moore");
  });
});

describe("runDispatch — failure routing", () => {
  // The one that matters most. A sender-side refusal is about OUR mailbox, so
  // recording it as a bounce would mark a perfectly reachable prospect
  // undeliverable forever on a fact about us.
  it("does NOT bounce the lead when Gmail throttles the SENDER", async () => {
    primeReads();
    mockDispatchMessage.mockRejectedValue(
      new SmtpDispatchError(
        "permanent",
        550,
        "550-5.4.5 Daily user sending limit exceeded",
        "refused",
      ),
    );

    const summary = await runDispatch({ asOf: NOW });

    expect(summary).toMatchObject({ senderBlocked: 1, bounced: 0, sent: 0 });
    expect(mockPromoteEvent).not.toHaveBeenCalled();

    // Still recorded — the refusal is real evidence, just not about the lead.
    const bronze = mockInsertValues.mock.calls[0]![0] as Record<string, unknown>;
    expect(bronze.outcome).toBe("permanent");
    expect(bronze.responseCode).toBe(550);
  });

  it("bounces the lead when the RECIPIENT is dead", async () => {
    primeReads();
    mockDispatchMessage.mockRejectedValue(
      new SmtpDispatchError("permanent", 550, "550 5.1.1 No such user here", "refused"),
    );

    const summary = await runDispatch({ asOf: NOW });

    expect(summary).toMatchObject({ bounced: 1, senderBlocked: 0 });
    const event = mockPromoteEvent.mock.calls[0]![0] as Record<string, unknown>;
    expect(event.eventType).toBe("email_bounced");
  });

  it("leaves a transient refusal to the next run, touching no lead state", async () => {
    primeReads();
    mockDispatchMessage.mockRejectedValue(
      new SmtpDispatchError("transient", 421, "421 4.7.0 Try again later", "deferred"),
    );

    const summary = await runDispatch({ asOf: NOW });

    expect(summary).toMatchObject({ transient: 1, sent: 0, bounced: 0 });
    expect(mockPromoteEvent).not.toHaveBeenCalled();
    expect((mockInsertValues.mock.calls[0]![0] as Record<string, unknown>).outcome).toBe(
      "transient",
    );
  });

  // Sending an empty email, or inventing content, is worse than not sending.
  it("skips a step with no persisted body rather than sending an empty email", async () => {
    primeReads({ hasBody: false });

    const summary = await runDispatch({ asOf: NOW });

    expect(summary).toMatchObject({ failed: 1, sent: 0 });
    expect(mockDispatchMessage).not.toHaveBeenCalled();
    expect(mockPromoteEvent).not.toHaveBeenCalled();
  });
});

describe("runDispatch — capacity", () => {
  it("sends nothing when no eligible sending account is returned", async () => {
    mockExecute
      .mockResolvedValueOnce({
        rows: [
          {
            instantlyCampaignId: "camp-1",
            leadEmail: "prospect@example.com",
            accountEmail: "amy@saviolabsco.com",
            provisionedSteps: [1],
            lastSentStep: null,
            lastSentAt: null,
            stepDelays: [],
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] });

    const summary = await runDispatch({ asOf: NOW });

    expect(summary).toMatchObject({ sequencesRead: 1, due: 0, sent: 0 });
    expect(mockDispatchMessage).not.toHaveBeenCalled();
  });
});
