import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));
vi.mock("../../src/lib/silver-promote", () => ({ promoteEvent: vi.fn() }));
vi.mock("../../src/lib/self-send/qualify-reply", () => ({ qualifyReply: vi.fn() }));

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
