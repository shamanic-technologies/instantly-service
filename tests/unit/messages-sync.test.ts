import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";

const mockExecute = vi.fn();
const mockInsertValues = vi.fn();
const mockOnConflict = vi.fn(async () => undefined);
vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockExecute(...a),
    insert: () => ({
      values: (...a: unknown[]) => {
        mockInsertValues(...a);
        return { onConflictDoUpdate: (...b: unknown[]) => mockOnConflict(...b) };
      },
    }),
  },
}));

import {
  MANUAL_REPLY_STEP,
  mapImapMessage,
  mapInstantlyEmail,
  mapSeedDispatch,
  mapSmtpDispatch,
  mapWarmupDispatch,
  syncMessages,
} from "../../src/lib/messages-sync";

/** What node-postgres actually returns — never a bare array. */
function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

describe("mapInstantlyEmail — the Unibox mirror", () => {
  const base = {
    id: "row-1",
    instantlyCampaignId: "camp-1",
    ueType: "1",
    messageId: "<abc@mail>",
    eaccount: "Amy@BoostDistribute.com",
    fromAddress: "amy@boostdistribute.com",
    toAddresses: "prospect@x.com",
    subject: "Hi",
    stepRaw: "0_1_0",
    timestampEmail: "2026-09-01T10:00:00Z",
    fetchedAt: new Date("2026-09-02T00:00:00Z"),
    leadEmail: "prospect@x.com",
    orgId: "org-1",
    campaignId: "caller-1",
    mailboxLogin: "amy@boostdistribute.com",
  };

  it("ue_type 1 is an outreach step, threaded on the sequence, step decoded from Instantly's encoding", () => {
    const m = mapInstantlyEmail(base)!;
    expect(m.direction).toBe("out");
    expect(m.kind).toBe("outreach");
    expect(m.transport).toBe("instantly");
    expect(m.step).toBe(2);
    expect(m.threadId).toBe("camp-1");
    expect(m.accountEmail).toBe("amy@boostdistribute.com");
    expect(m.counterparty).toBe("prospect@x.com");
    expect(m.occurredAt).toEqual(new Date("2026-09-01T10:00:00Z"));
    expect(m.outcome).toBe("sent");
  });

  it("ue_type 2 is an inbound reply from the prospect's OWN address, which may differ from the lead's", () => {
    const m = mapInstantlyEmail({ ...base, ueType: "2", fromAddress: "assistant@x.com", stepRaw: null })!;
    expect(m.direction).toBe("in");
    expect(m.kind).toBe("reply");
    expect(m.counterparty).toBe("assistant@x.com");
    expect(m.step).toBeNull();
    expect(m.outcome).toBe("received");
  });

  it("ue_type 3/4 is a reply WE sent through Instantly, never a sequence step", () => {
    expect(mapInstantlyEmail({ ...base, ueType: "3" })!.kind).toBe("manual_reply");
  });

  it("returns null rather than inventing an account", () => {
    expect(mapInstantlyEmail({ ...base, eaccount: null, fromAddress: null })).toBeNull();
  });
});

describe("mapSmtpDispatch — our own dispatcher", () => {
  const base = {
    id: "d-1",
    instantlyCampaignId: "self:abc",
    leadEmail: "P@x.com",
    accountEmail: "kevin@marketingagency.life",
    step: 2,
    outcome: "sent",
    messageId: "<m1@x>",
    subject: "Re: Hi",
    dispatchedAt: new Date("2026-09-03T09:00:00Z"),
    orgId: "org-1",
    campaignId: "caller-1",
    mailboxLogin: "kevin@marketingagency.life",
  };

  it("a sequence step keeps its step and threads on the sequence", () => {
    const m = mapSmtpDispatch(base)!;
    expect(m.kind).toBe("outreach");
    expect(m.step).toBe(2);
    expect(m.threadId).toBe("self:abc");
    expect(m.counterparty).toBe("p@x.com");
    expect(m.transport).toBe("smtp");
  });

  it("step 0 is a one-to-one reply, with NO step — never a sequence step", () => {
    const m = mapSmtpDispatch({ ...base, step: MANUAL_REPLY_STEP })!;
    expect(m.kind).toBe("manual_reply");
    expect(m.step).toBeNull();
  });

  it("keeps a refused attempt as evidence, with its outcome", () => {
    expect(mapSmtpDispatch({ ...base, outcome: "permanent" })!.outcome).toBe("permanent");
    expect(mapSmtpDispatch({ ...base, outcome: "weird" })!.outcome).toBe("transient");
  });

  it("MANUAL_REPLY_STEP mirrors its one declaration", () => {
    // The constant moved into its own module so the human-takeover gate could
    // read it without importing `reply-to-lead`, which imports the gate. This
    // pins the declaration at its new home AND that `reply-to-lead` still
    // re-exports it, so every existing importer keeps working.
    const src = readFileSync("src/lib/manual-reply-step.ts", "utf8");
    expect(src).toContain(`export const MANUAL_REPLY_STEP = ${MANUAL_REPLY_STEP};`);

    const replyToLead = readFileSync("src/lib/reply-to-lead.ts", "utf8");
    expect(replyToLead).toContain("export { MANUAL_REPLY_STEP };");
  });
});

describe("mapImapMessage — what came back", () => {
  const base = {
    id: "i-1",
    accountEmail: "kevin@marketingagency.life",
    messageId: "<r1@x>",
    fromAddress: "Prospect <p@x.com>",
    subject: "Re: Hi",
    kind: "reply",
    instantlyCampaignId: "self:abc",
    step: 1,
    receivedAt: new Date("2026-09-04T08:00:00Z"),
    polledAt: new Date("2026-09-04T09:00:00Z"),
    orgId: "org-1",
    campaignId: "caller-1",
    mailboxLogin: "kevin@marketingagency.life",
  };

  it("a correlated reply threads on its sequence; kinds pass through", () => {
    expect(mapImapMessage(base)!.threadId).toBe("self:abc");
    expect(mapImapMessage({ ...base, kind: "bounce" })!.kind).toBe("bounce");
    expect(mapImapMessage({ ...base, kind: "auto_reply" })!.kind).toBe("auto_reply");
  });

  it("an unrelated message is kept, threaded on itself — it is the record of what we ignored", () => {
    const m = mapImapMessage({ ...base, kind: "unrelated", instantlyCampaignId: null, step: null })!;
    expect(m.kind).toBe("unrelated");
    expect(m.threadId).toBe("<r1@x>");
  });

  it("falls back to the poll time when the message carries no date", () => {
    expect(mapImapMessage({ ...base, receivedAt: null })!.occurredAt).toEqual(new Date("2026-09-04T09:00:00Z"));
  });
});

describe("mapWarmupDispatch / mapSeedDispatch — the other two typologies", () => {
  it("a warmup send threads on itself, carries the day and its landing folder; a Re: is a warmup reply", () => {
    const base = {
      id: "w-1",
      senderEmail: "a@x.com",
      senderMailbox: "a@x.com",
      receiverEmail: "b@y.com",
      dayKey: "2026-09-05",
      messageId: "<w1@x>",
      subject: "Quick question WRM-1234",
      outcome: "sent",
      dispatchedAt: new Date("2026-09-05T07:00:00Z"),
      placement: "spam",
    };
    const m = mapWarmupDispatch(base)!;
    expect(m.kind).toBe("warmup");
    expect(m.threadId).toBe("<w1@x>");
    expect(m.contextRef).toBe("2026-09-05");
    expect(m.placement).toBe("spam");
    expect(m.mailboxLogin).toBe("a@x.com");
    expect(mapWarmupDispatch({ ...base, subject: "Re: Quick question WRM-1234" })!.kind).toBe("warmup_reply");
  });

  it("a seed carries its test id, placement and the receiver's auth verdict (null when not reported)", () => {
    const m = mapSeedDispatch({
      id: "s-1",
      testId: "seed:t1",
      senderEmail: "a@x.com",
      receiverEmail: "seed@gmail.com",
      messageId: "<s1@x>",
      outcome: "sent",
      dispatchedAt: new Date("2026-09-06T06:00:00Z"),
      placement: "inbox",
      spfPass: true,
      dkimPass: "f",
      dmarcPass: null,
      mailboxLogin: "a@x.com",
    })!;
    expect(m.kind).toBe("seed");
    expect(m.contextRef).toBe("seed:t1");
    expect(m.placement).toBe("inbox");
    expect(m.spfPass).toBe(true);
    expect(m.dkimPass).toBe(false);
    expect(m.dmarcPass).toBeNull();
  });
});

describe("syncMessages — IO", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockOnConflict.mockResolvedValue(undefined);
    mockExecute
      .mockResolvedValueOnce(
        pgResult([
          { id: "e1", instantly_campaign_id: "camp-1", fetched_at: new Date("2026-09-02"), ue_type: "1", message_id: "<a>", eaccount: "amy@b.com", from_address: null, to_addresses: "p@x.com", subject: "Hi", step_raw: "0_0_0", timestamp_email: "2026-09-01T10:00:00Z", lead_email: "p@x.com", org_id: "o", campaign_id: "c", mailbox_login: "amy@b.com" },
          { id: "e2", instantly_campaign_id: "camp-1", fetched_at: new Date("2026-09-02"), ue_type: "1", message_id: null, eaccount: null, from_address: null, to_addresses: null, subject: null, step_raw: null, timestamp_email: null, lead_email: null, org_id: null, campaign_id: null, mailbox_login: null },
        ]),
      )
      .mockResolvedValueOnce(pgResult([{ id: "d1", instantly_campaign_id: "self:1", lead_email: "p@x.com", account_email: "k@m.life", step: 1, outcome: "sent", message_id: "<d>", dispatched_at: new Date("2026-09-03"), subject: "Hi", org_id: "o", campaign_id: "c", mailbox_login: "kevin@m.life" }]))
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([{ id: "w1", sender_email: "a@x.com", sender_mailbox: "a@x.com", receiver_email: "b@y.com", day_key: "2026-09-05", message_id: "<w>", subject: "s", outcome: "sent", dispatched_at: new Date("2026-09-05"), placement: null }]))
      .mockResolvedValueOnce(pgResult([]));
  });

  it("reads five sources through the QueryResult shape, upserts what maps, counts what does not", async () => {
    const summary = await syncMessages({ sinceDays: 7 });
    expect(summary.windowDays).toBe(7);
    expect(summary.read).toEqual({
      instantly_emails_raw: 2,
      smtp_dispatch_raw: 1,
      imap_messages_raw: 0,
      warmup_dispatches: 1,
      seed_placement_dispatches: 0,
    });
    expect(summary.upserted).toBe(3);
    expect(summary.skipped).toBe(1);
    expect(mockInsertValues).toHaveBeenCalledTimes(1);
    const rows = mockInsertValues.mock.calls[0][0] as Array<{ sourceTable: string; sourceRowId: string }>;
    expect(rows.map((r) => `${r.sourceTable}/${r.sourceRowId}`)).toEqual([
      "instantly_emails_raw/e1",
      "smtp_dispatch_raw/d1",
      "warmup_dispatches/w1",
    ]);
  });

  it("excludes `unrelated` inbound at the READ — 80,697 of 80,891 rows in three days were newsletters", async () => {
    await syncMessages({ sinceDays: 2 });
    const imapQuery = JSON.stringify((mockExecute.mock.calls[2][0] as { queryChunks?: unknown[] }).queryChunks);
    expect(imapQuery).toContain("FROM imap_messages_raw");
    expect(imapQuery).toContain("kind <> 'unrelated'");
  });

  it("bounds every source read on the window", async () => {
    await syncMessages({ sinceDays: 2 });
    for (const call of mockExecute.mock.calls) {
      const q = call[0] as { queryChunks?: unknown[] };
      const text = JSON.stringify(q.queryChunks ?? q);
      expect(text).toContain("make_interval");
    }
  });

  it("defaults the window to 3 days and refuses nothing silently — a read failure throws", async () => {
    mockExecute.mockReset();
    mockExecute.mockRejectedValueOnce(new Error("db down"));
    await expect(syncMessages()).rejects.toThrow("db down");
    expect(mockInsertValues).not.toHaveBeenCalled();
  });
});
