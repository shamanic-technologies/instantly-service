/**
 * A message one of OUR OWN people wrote is never the prospect's reply.
 *
 * Prod 2026-09-21/22: kevin@distribute.you answered two prospects from Gmail and
 * CC'd the sending mailbox. Instantly filed both `ue_type 2`, and every reader
 * took them for replies from the lead — the messages projection, the thread the
 * responder reads, the message an automated answer threads onto, and the
 * takeover gate (which then could not see that a person had taken over).
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("../../src/db", () => ({ db: { execute: vi.fn() } }));

import {
  bareAddress,
  isStaffSender,
  staffDomains,
  staffSenderSql,
} from "../../src/lib/staff-senders";
import { refileStaffReply, STAFF_REPLY_KIND, eventTypeForInbound } from "../../src/lib/self-send/inbound";
import { mapImapMessage, mapInstantlyEmail } from "../../src/lib/messages-sync";
import { selectThreadMessages } from "../../src/lib/forward-positive-reply";
import { selectReplyTarget } from "../../src/lib/reply-to-lead";
import type { EmailRecord } from "../../src/lib/instantly-client";

function sqlText(obj: unknown): string {
  if (typeof obj === "string") return obj;
  if (obj == null) return "";
  if (Array.isArray(obj)) return obj.map(sqlText).join("");
  if (typeof obj === "object") {
    const o = obj as Record<string, unknown>;
    if (Array.isArray(o.value)) return o.value.join("");
    if (Array.isArray(o.queryChunks)) return sqlText(o.queryChunks);
    return Object.values(o).map(sqlText).join("");
  }
  return "";
}

const saved = process.env.ADMIN_NOTIFICATION_EMAIL;
beforeEach(() => {
  delete process.env.ADMIN_NOTIFICATION_EMAIL;
});
afterEach(() => {
  if (saved === undefined) delete process.env.ADMIN_NOTIFICATION_EMAIL;
  else process.env.ADMIN_NOTIFICATION_EMAIL = saved;
});

describe("who our own people are", () => {
  it("is anyone on a staff domain, the agency inbox's included", () => {
    expect(isStaffSender("kevin@distribute.you")).toBe(true);
    expect(isStaffSender("Kevin Lourd <Kevin@Distribute.you>")).toBe(true);
    expect(isStaffSender("sarah@distribute.you")).toBe(true);
    process.env.ADMIN_NOTIFICATION_EMAIL = "ops@agency.example";
    expect(staffDomains()).toEqual(expect.arrayContaining(["distribute.you", "agency.example"]));
    expect(isStaffSender("ops@agency.example")).toBe(true);
  });

  it("is never a prospect, nor our cold sending domains", () => {
    expect(isStaffSender("jamie@kinetikchaindenver.com")).toBe(false);
    // A cold sending mailbox is recorded as outbound where it sends; treating
    // the cold estate as staff would reclassify warmup traffic.
    expect(isStaffSender("nina@veriskube.com")).toBe(false);
    expect(isStaffSender("kevin.l@maildistribute.com")).toBe(false);
  });

  it("is never the lead themself, whatever domain they sit on", () => {
    expect(isStaffSender("test@distribute.you", "Test@distribute.you")).toBe(false);
  });

  it("reads nothing into a sender it cannot parse", () => {
    expect(isStaffSender(null)).toBe(false);
    expect(isStaffSender("")).toBe(false);
    expect(bareAddress("no address here")).toBeNull();
  });

  it("the SQL predicate matches on the domain of the stored address", () => {
    const text = sqlText(staffSenderSql({ queryChunks: ["x"] } as never));
    expect(text).toContain("split_part");
    expect(text).toContain("IN (");
  });
});

describe("the IMAP poller files a CC'd staff answer as ours", () => {
  it("re-files a correlated reply, and promotes nothing for it", () => {
    expect(refileStaffReply("reply", true)).toBe(STAFF_REPLY_KIND);
    expect(eventTypeForInbound(STAFF_REPLY_KIND)).toBeNull();
  });

  it("leaves a prospect's reply, and a bounce from our own domain, alone", () => {
    expect(refileStaffReply("reply", false)).toBe("reply");
    expect(refileStaffReply("bounce", true)).toBe("bounce");
    expect(refileStaffReply("auto_reply", true)).toBe("auto_reply");
  });
});

describe("the messages projection records it as an outbound reply sent by a human", () => {
  const base = {
    id: "999c0ea8-7b38-4c41-b8f9-d64a145c3af6",
    instantlyCampaignId: "e3c917a5-83fe-460f-a5da-49125ad0fa0b",
    ueType: "2",
    messageId: "<m1@mail.gmail.com>",
    eaccount: "nina@veriskube.com",
    toAddresses: "jamie@kinetikchaindenver.com",
    subject: "Re: quick question",
    stepRaw: null,
    timestampEmail: "2026-09-21T19:07:27.000Z",
    fetchedAt: new Date("2026-09-21T19:10:00Z"),
    leadEmail: "jamie@kinetikchaindenver.com",
    orgId: "org-1",
    campaignId: "camp-1",
    mailboxLogin: null,
  };

  it("kevin@distribute.you CC'd on the thread → out / manual_reply / sent", () => {
    const row = mapInstantlyEmail({ ...base, fromAddress: "kevin@distribute.you" });
    expect(row).toMatchObject({
      direction: "out",
      kind: "manual_reply",
      outcome: "sent",
      counterparty: "jamie@kinetikchaindenver.com",
      accountEmail: "nina@veriskube.com",
    });
  });

  it("the prospect's own reply is still in / reply", () => {
    const row = mapInstantlyEmail({ ...base, fromAddress: "jamie@kinetikchaindenver.com" });
    expect(row).toMatchObject({ direction: "in", kind: "reply", outcome: "received" });
  });

  it("the self-send transport's staff_reply is out / manual_reply too", () => {
    const row = mapImapMessage({
      id: "i1",
      accountEmail: "kevin.l@maildistribute.com",
      messageId: "<m2@x>",
      fromAddress: "Kevin <kevin@distribute.you>",
      subject: "Re: x",
      kind: STAFF_REPLY_KIND,
      instantlyCampaignId: "self:abc",
      step: 2,
      receivedAt: new Date("2026-09-28T10:00:00Z"),
      polledAt: new Date("2026-09-28T10:01:00Z"),
      orgId: "org-1",
      campaignId: "camp-1",
      mailboxLogin: null,
    });
    expect(row).toMatchObject({ direction: "out", kind: "manual_reply", outcome: "sent" });
  });
});

function email(over: Partial<EmailRecord>): EmailRecord {
  return {
    id: "e1",
    campaign_id: "ic-1",
    lead: "jamie@kinetikchaindenver.com",
    lead_id: null,
    eaccount: "nina@veriskube.com",
    ue_type: 2,
    step: "1",
    subject: "Re: quick question",
    timestamp_email: "2026-09-21T15:00:00.000Z",
    ...over,
  } as EmailRecord;
}

describe("the thread the responder reads, and the message it answers", () => {
  const records = [
    email({ id: "out-1", ue_type: 1, from_address_email: "nina@veriskube.com", timestamp_email: "2026-09-20T10:00:00.000Z" }),
    email({ id: "in-lead", from_address_email: "jamie@kinetikchaindenver.com", timestamp_email: "2026-09-21T12:43:00.000Z" }),
    email({ id: "in-staff", from_address_email: "kevin@distribute.you", timestamp_email: "2026-09-21T19:07:27.000Z" }),
  ];

  it("renders the staff message as OUR side of the conversation", () => {
    const thread = selectThreadMessages(records);
    expect(thread.map((m) => m.direction)).toEqual(["outbound", "inbound", "outbound"]);
  });

  it("threads an automated answer onto the PROSPECT's latest message, never our colleague's", () => {
    expect(selectReplyTarget(records)?.emailId).toBe("in-lead");
  });
});
