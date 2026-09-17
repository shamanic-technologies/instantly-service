/**
 * The cadence fix (2026-09-17): what makes a due step go out on time.
 *
 * Three properties, each of which was either absent or unenforced:
 *   - two sweeps never run the same selection concurrently (no double sends),
 *   - a run that can send nothing does not pay for the mailbox read,
 *   - a run that sends anything still reads the mailboxes FIRST.
 *
 * Plus the poller's fan-out, whose unit is the real mailbox login rather than
 * the sending address.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockRunPoll = vi.fn();
const mockLoadMailboxLogins = vi.fn();
const mockDispatchScheduledReplies = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
    insert: () => ({
      values: () => ({ returning: () => Promise.resolve([{ id: "bronze-1" }]) }),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  smtpDispatchRaw: { id: "id" },
  scheduledReplies: { id: "id", scheduledFor: "scheduled_for" },
}));

vi.mock("../../src/lib/self-send/imap-poller", () => ({
  runPoll: (...args: unknown[]) => mockRunPoll(...args),
}));

vi.mock("../../src/lib/scheduled-replies-worker", () => ({
  dispatchScheduledReplies: (...args: unknown[]) =>
    mockDispatchScheduledReplies(...args),
}));

vi.mock("../../src/lib/self-send/mailbox-credentials", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  loadMailboxLogins: (...args: unknown[]) => mockLoadMailboxLogins(...args),
  resolveMailboxCredential: async () => ({
    address: "amy@saviolabsco.com",
    appPassword: "pw",
    smtpHost: "smtp.gmail.com",
    imapHost: "imap.gmail.com",
  }),
}));

vi.mock("../../src/lib/self-send/smtp", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  dispatchMessage: async () => ({ messageId: "<m@x>", response: "250 ok", rejected: [] }),
}));

vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: async () => ({ promoted: true, silverEventId: "ev-1" }),
}));

process.env.SELF_SEND_UNSUBSCRIBE_SECRET = "cadence-test-secret";
process.env.SELF_SEND_PUBLIC_URL = "https://opt.test";

const { runDispatch, __resetDispatchInFlight } = await import(
  "../../src/lib/self-send/dispatch-worker"
);

/** Monday 10:00 America/Chicago — a sending day, inside the prospect's window. */
const NOW = new Date("2026-08-17T15:00:00Z");

const SEQUENCE_ROW = {
  instantlyCampaignId: "camp-1",
  leadEmail: "prospect@example.com",
  accountEmail: "amy@saviolabsco.com",
  provisionedSteps: [1],
  lastSentStep: null,
  lastSentAt: null,
  stepDelays: [],
  timezone: "America/Chicago",
};

const ACCOUNT_ROW = {
  accountEmail: "amy@saviolabsco.com",
  firstName: "Amy",
  lastName: "Moore",
  dailyLimit: 45,
  sentToday: 0,
};

/** One plan cycle: sequences, accounts, the ramp's volume read. */
function primePlan(sequences: unknown[], accounts: unknown[]) {
  mockExecute
    .mockResolvedValueOnce({ rows: sequences })
    .mockResolvedValueOnce({ rows: accounts })
    .mockResolvedValueOnce({
      rows: accounts.map(() => ({ accountEmail: "amy@saviolabsco.com", peak: 45 })),
    });
}

/** The waiting-reply probe. */
function primeReplyProbe() {
  mockExecute.mockResolvedValueOnce({ rows: [] });
}

/** The per-step body read. */
function primeStepContent() {
  mockExecute.mockResolvedValueOnce({
    rows: [{ bodyHtml: "<p>Hi</p>", subject: "Quick question", priorMessageIds: [] }],
  });
}

beforeEach(() => {
  vi.resetAllMocks();
  __resetDispatchInFlight();
  mockLoadMailboxLogins.mockResolvedValue(
    new Map([["amy@saviolabsco.com", "amy@saviolabsco.com"]]),
  );
  mockRunPoll.mockResolvedValue({ accountsPolled: 1 });
  mockDispatchScheduledReplies.mockResolvedValue({
    pending: 0,
    due: 0,
    sent: 0,
    failed: 0,
  });
});

describe("runDispatch — one sweep at a time", () => {
  it("refuses a second concurrent sweep instead of selecting the same steps twice", async () => {
    // The queue is the set of still-`provisioned` holds, and a hold only leaves
    // it once its `email_sent` has been promoted — so two overlapping sweeps
    // genuinely pick the same step and genuinely send the prospect two copies.
    let release: (() => void) | null = null;
    mockLoadMailboxLogins.mockImplementation(
      () =>
        new Promise((resolve) => {
          release = () =>
            resolve(new Map([["amy@saviolabsco.com", "amy@saviolabsco.com"]]));
        }),
    );

    const first = runDispatch({ asOf: NOW });
    // Let the first run reach its (blocked) credential read.
    await new Promise((r) => setTimeout(r, 0));

    const second = await runDispatch({ asOf: NOW });
    expect(second.skippedConcurrent).toBe(true);
    expect(second.sent).toBe(0);
    expect(second.polled).toBe(false);

    primePlan([], []);
    primeReplyProbe();
    release?.();
    await first;
  });

  it("is available again once the previous sweep has finished", async () => {
    primePlan([], []);
    primeReplyProbe();
    await runDispatch({ asOf: NOW });

    primePlan([], []);
    primeReplyProbe();
    const second = await runDispatch({ asOf: NOW });
    expect(second.skippedConcurrent).toBe(false);
  });
});

describe("runDispatch — the probe that decides whether to read the mailboxes", () => {
  it("does NOT poll when nothing is sendable, and still reports the backlog", async () => {
    // A fleet at its daily cap. This is the common tick: without the probe a
    // ten-minute interval would read every mailbox around the clock to discover
    // there was nothing to do.
    primePlan([SEQUENCE_ROW], [{ ...ACCOUNT_ROW, sentToday: 45 }]);
    primeReplyProbe();

    const summary = await runDispatch({ asOf: NOW, pollFirst: true });

    expect(mockRunPoll).not.toHaveBeenCalled();
    expect(summary.polled).toBe(false);
    expect(summary.sent).toBe(0);
    // The step was due and simply had no room — the distinction that made a
    // throttled fleet read as an idle one.
    expect(summary.dueBeforeCapacity).toBe(1);
  });

  it("polls BEFORE the selection it acts on when there is something to send", async () => {
    const order: string[] = [];
    mockRunPoll.mockImplementation(async () => {
      order.push("poll");
      return { accountsPolled: 1 };
    });

    primePlan([SEQUENCE_ROW], [ACCOUNT_ROW]); // probe
    primeReplyProbe();
    primePlan([SEQUENCE_ROW], [ACCOUNT_ROW]); // re-selection, after the poll
    primeStepContent();

    const summary = await runDispatch({ asOf: NOW, pollFirst: true });

    expect(order).toEqual(["poll"]);
    expect(mockRunPoll).toHaveBeenCalledTimes(1);
    expect(summary.polled).toBe(true);
    expect(summary.sent).toBe(1);
  });

  it("drops a step the poll stopped, because the selection is re-taken afterwards", async () => {
    // The whole point of reading first: a prospect who answered since the last
    // sweep is out of the queue before we pick what to send.
    primePlan([SEQUENCE_ROW], [ACCOUNT_ROW]); // probe: one step due
    primeReplyProbe();
    primePlan([], [ACCOUNT_ROW]); // after the poll: the reply stopped it

    const summary = await runDispatch({ asOf: NOW, pollFirst: true });

    expect(summary.polled).toBe(true);
    expect(summary.due).toBe(0);
    expect(summary.sent).toBe(0);
  });

  it("polls for a waiting reply even when no sequence step is due", async () => {
    primePlan([], [ACCOUNT_ROW]);
    mockExecute.mockResolvedValueOnce({
      rows: [
        {
          id: "r-1",
          orgId: "org-1",
          userId: "user-1",
          campaignId: "cc-1",
          instantlyCampaignId: "camp-1",
          leadEmail: "prospect@example.com",
          bodyHtml: "<p>yes</p>",
          timezone: "America/Chicago",
          scheduledFor: "2026-08-17T14:00:00Z",
          attempts: 0,
        },
      ],
    });
    primePlan([], [ACCOUNT_ROW]); // re-selection after the poll

    mockDispatchScheduledReplies.mockResolvedValue({
      pending: 1,
      due: 1,
      sent: 1,
      failed: 0,
    });

    const summary = await runDispatch({ asOf: NOW, pollFirst: true });

    expect(mockRunPoll).toHaveBeenCalledTimes(1);
    expect(summary.repliesSent).toBe(1);
  });
});

describe("the interval that replaced the cron", () => {
  it("states its bound explicitly and defaults to ten minutes", async () => {
    const { SELF_SEND_DISPATCH_INTERVAL_MS } = await import(
      "../../src/lib/self-send/dispatch-scheduler"
    );
    // The delay a due step can wait before a sweep looks at it. GitHub Actions
    // delivered 6.2 runs a day against 24 declared, with gaps of 2.5h to 5.7h.
    expect(SELF_SEND_DISPATCH_INTERVAL_MS).toBe(10 * 60_000);
  });
});
