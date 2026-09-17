import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));
vi.mock("../../src/lib/silver-promote", () => ({ promoteEvent: vi.fn() }));
vi.mock("../../src/lib/self-send/qualify-reply", () => ({ qualifyReply: vi.fn() }));
// The poll groups its accounts by real mailbox login before reading any of them,
// so the credential map is on the path of every run — including one with no
// accounts at all. Mocked partially: the rest of the module (the IMAP port, the
// per-mailbox resolution) is the real thing.
const mockMailboxLogins = vi.fn(async () => new Map<string, string>());
const mockResolveCredential = vi.fn();
vi.mock("../../src/lib/self-send/mailbox-credentials", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  loadMailboxLogins: (...a: unknown[]) => mockMailboxLogins(...(a as [])),
  resolveMailboxCredential: (...a: unknown[]) =>
    mockResolveCredential(...(a as [string])),
}));

import { loadKnownSends } from "../../src/lib/self-send/imap-poller";

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

beforeEach(() => {
  vi.resetAllMocks();
});

/**
 * The correlation key on a mailbox whose sequences INSTANTLY sent.
 *
 * Without this second source there is no `smtp_dispatch_raw` row to match, so
 * every reply on such a mailbox classifies `unrelated` and touches nothing —
 * which is exactly how a "I would be interested" reply reached a customer's
 * personal Gmail and nothing else in the fleet.
 */
describe("loadKnownSends", () => {
  it("unions our own dispatches with what Instantly sent from the same mailbox", async () => {
    mockDbExecute
      .mockResolvedValueOnce(
        pgResult([
          {
            messageId: "<ours@boostdistribute.com>",
            instantlyCampaignId: "self:aaa",
            leadEmail: "a@x.com",
            step: 1,
          },
        ]),
      )
      .mockResolvedValueOnce(
        pgResult([
          {
            messageId: "<theirs@marketingagency.network>",
            instantlyCampaignId: "e1e216ca-635a-4682-92be-f5057f8224ea",
            leadEmail: "jason@uhmedical.com",
            rawStep: "0_1_0",
          },
        ]),
      );

    const sends = await loadKnownSends("kevin.lourd@marketingagency.network");

    expect(sends.size).toBe(2);
    expect(sends.get("<theirs@marketingagency.network>")).toEqual({
      instantlyCampaignId: "e1e216ca-635a-4682-92be-f5057f8224ea",
      leadEmail: "jason@uhmedical.com",
      // Instantly's `0_1_0` is our step 2.
      step: 2,
      // Carried so a reply can pause the sequence on Instantly's side — the
      // mirror row's campaign join supplies it.
      orgId: null,
    });
  });

  it("drops an Instantly send whose step cannot be read rather than defaulting it", async () => {
    // A wrong step makes the inference rule project an `email_sent` for a step
    // nobody sent.
    mockDbExecute.mockResolvedValueOnce(pgResult([])).mockResolvedValueOnce(
      pgResult([
        {
          messageId: "<x@y.com>",
          instantlyCampaignId: "c1",
          leadEmail: "a@x.com",
          rawStep: null,
        },
      ]),
    );

    const sends = await loadKnownSends("kevin@x.com");

    expect(sends.size).toBe(0);
  });

  it("lets our OWN dispatch win a message-id collision", async () => {
    mockDbExecute
      .mockResolvedValueOnce(
        pgResult([
          { messageId: "<dup@x.com>", instantlyCampaignId: "self:aaa", leadEmail: "a@x.com", step: 3 },
        ]),
      )
      .mockResolvedValueOnce(
        pgResult([
          { messageId: "<dup@x.com>", instantlyCampaignId: "other", leadEmail: "b@x.com", rawStep: "0_0_0" },
        ]),
      );

    const sends = await loadKnownSends("kevin@x.com");

    expect(sends.get("<dup@x.com>")).toMatchObject({ instantlyCampaignId: "self:aaa", step: 3 });
  });

  it("keys on Instantly's own message_id — never on the sender address", async () => {
    // The prospect frequently replies from a DIFFERENT address than the one we
    // mailed, and the same prospect can sit in two sequences, so an address
    // match is wrong in both directions.
    mockDbExecute.mockResolvedValue(pgResult([]));

    await loadKnownSends("kevin@x.com");

    const instantlyQuery = JSON.stringify(mockDbExecute.mock.calls[1][0]);
    expect(instantlyQuery).toContain("instantly_emails_raw");
    expect(instantlyQuery).toContain("message_id");
    expect(instantlyQuery).toContain("eaccount");
    expect(instantlyQuery).toContain("ue_type");
    expect(instantlyQuery).not.toContain("from_address_email");
  });
});

/**
 * The catch-up window.
 *
 * A mailbox nobody has ever read holds replies far older than the routine
 * 3-day window, and for those mailboxes the mail exists nowhere else — Instantly
 * could not log in either, so its Unibox never mirrored them.
 */
describe("runPoll window", () => {
  it("clamps sinceDays into [routine, max] and never below the routine window", async () => {
    const { runPoll } = await import("../../src/lib/self-send/imap-poller");
    mockDbExecute.mockResolvedValue(pgResult([]));

    // No accounts on the transport, so the run is a no-op — what is under test
    // is that an absurd or hostile value cannot widen or narrow it wrongly.
    await expect(runPoll({ sinceDays: 100_000 })).resolves.toMatchObject({
      accountsPolled: 0,
    });
    await expect(runPoll({ sinceDays: -5 })).resolves.toMatchObject({ accountsPolled: 0 });
    await expect(runPoll({ sinceDays: Number.NaN })).resolves.toMatchObject({
      accountsPolled: 0,
    });
  });
});

describe("runPoll fan-out", () => {
  it("reads distinct mailboxes in parallel and a mailbox's aliases in sequence", async () => {
    // ⚠️ The unit is the SASL login, not the sending address. A Gandi domain is
    // one mailbox behind several aliases, so a flat fan-out over accounts would
    // open several simultaneous sessions as the SAME user — which is what a
    // relay refuses. Sequentially over 249 accounts this poll measured 45-80
    // MINUTES in production, and it sits on the critical path of every send.
    const { runPoll } = await import("../../src/lib/self-send/imap-poller");

    mockDbExecute.mockResolvedValue(
      pgResult([
        { email: "kevin@molthost.org" },
        { email: "klourd@molthost.org" },
        { email: "amy@saviolabsco.com" },
      ]),
    );
    mockMailboxLogins.mockResolvedValue(
      new Map([
        ["kevin@molthost.org", "kevin@molthost.org"],
        ["klourd@molthost.org", "kevin@molthost.org"],
        ["amy@saviolabsco.com", "amy@saviolabsco.com"],
      ]),
    );

    const live = new Map<string, number>();
    const peak = new Map<string, number>();
    mockResolveCredential.mockImplementation(async (address: string) => {
      const login = address.endsWith("@molthost.org")
        ? "kevin@molthost.org"
        : address;
      const now = (live.get(login) ?? 0) + 1;
      live.set(login, now);
      peak.set(login, Math.max(peak.get(login) ?? 0, now));
      await new Promise((r) => setTimeout(r, 5));
      live.set(login, (live.get(login) ?? 1) - 1);
      // Refusing here keeps the test about the SCHEDULING: each account is
      // counted as failed, never as read, and the sweep continues.
      throw new Error("no IMAP in a unit test");
    });

    const summary = await runPoll();

    expect(summary.accountsFailed).toBe(3);
    // Never two sessions as one login...
    expect(peak.get("kevin@molthost.org")).toBe(1);
    // ...and the two mailboxes genuinely overlapped rather than queueing.
    expect(peak.get("amy@saviolabsco.com")).toBe(1);
    expect(mockResolveCredential).toHaveBeenCalledTimes(3);
  });
});
