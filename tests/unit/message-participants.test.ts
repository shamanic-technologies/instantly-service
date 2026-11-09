import { describe, it, expect, vi } from "vitest";

vi.mock("../../src/db", () => ({ db: { execute: vi.fn(), transaction: vi.fn() } }));

import {
  dispatchEnvelope,
  imapEnvelope,
  instantlyEnvelope,
  parseAddressHeader,
  parseMessageIdList,
} from "../../src/lib/message-participants";
import { buildDispatchMessage, buildImapMessage, imapMessageKey } from "../../src/lib/mailbox-messages";
import { factEnvelope } from "../../src/lib/outreach-facts";
import { MARKUS_IMAP_ROW } from "../helpers/markus-reply";

describe("parseAddressHeader reads every stored shape", () => {
  it("a mailparser object serialised to a JSON string (IMAP bronze)", () => {
    const raw = JSON.stringify({
      value: [
        { address: "Mario@AppLandTech.com", name: "Mario" },
        { address: "kevin@distribute.you", name: "" },
      ],
    });
    expect(parseAddressHeader(raw)).toEqual([
      { address: "mario@applandtech.com", name: "Mario" },
      { address: "kevin@distribute.you", name: null },
    ]);
  });

  it("header text with quoted names and commas inside the quotes", () => {
    expect(parseAddressHeader('"Schmid, Markus" <markus@x.ch>, thomas@x.ch')).toEqual([
      { address: "markus@x.ch", name: "Schmid, Markus" },
      { address: "thomas@x.ch", name: null },
    ]);
  });

  it("a comma list (our dispatch log, Instantly mirror), deduplicated", () => {
    expect(parseAddressHeader("kevin@distribute.you,dev@docdinners.com,KEVIN@distribute.you")).toEqual([
      { address: "kevin@distribute.you", name: null },
      { address: "dev@docdinners.com", name: null },
    ]);
  });

  it("nothing usable yields nothing, never a throw", () => {
    expect(parseAddressHeader(null)).toEqual([]);
    expect(parseAddressHeader("")).toEqual([]);
    expect(parseAddressHeader("{not json")).toEqual([]);
    expect(parseAddressHeader({ value: [{ group: [{ address: "a@b.co" }] }] })).toEqual([
      { address: "a@b.co", name: null },
    ]);
  });

  it("References as a JSON array string or header text, oldest first", () => {
    expect(parseMessageIdList('["<a@x>","<b@x>"]')).toEqual(["<a@x>", "<b@x>"]);
    expect(parseMessageIdList("<a@x> <b@x>")).toEqual(["<a@x>", "<b@x>"]);
  });
});

describe("Markus answering in Thomas's thread reads as Markus (EVERY-PERSON-ON-THE-THREAD-1109)", () => {
  it("the IMAP envelope names Markus as the writer and our mailbox as the recipient", () => {
    const env = imapEnvelope({
      accountEmail: MARKUS_IMAP_ROW.account_email,
      fromAddress: MARKUS_IMAP_ROW.from_address,
      messageId: MARKUS_IMAP_ROW.message_id,
      payload: MARKUS_IMAP_ROW.payload,
    });
    expect(env.from).toEqual({ address: "markus@markthalle-luzern.ch", name: "Markus Schmid" });
    expect(env.participants).toEqual([
      { email: "markus@markthalle-luzern.ch", role: "from" },
      { email: "kevin@marketingagency.group", role: "to" },
    ]);
    expect(env.inReplyTo).toBe("<CAPfU9LnYs30w-Q30SrLD87Ase-eBjUb4K64_JsAwSjXCHHfwyA@mail.gmail.com>");
    expect(env.threadKey).toBe("<5ac53340-e14c-ac34-7685-52a54368fbe2@marketingagency.group>");
  });

  it("the feed serves it from Markus, filed on Thomas's thread (thread context kept)", () => {
    const m = buildImapMessage(MARKUS_IMAP_ROW);
    expect(m.id).toBe(imapMessageKey(MARKUS_IMAP_ROW.message_id));
    expect(m.direction).toBe("in");
    expect(m.kind).toBe("reply");
    expect(m.from?.address).toBe("markus@markthalle-luzern.ch");
    expect(m.leadEmail).toBe("thomas@markthalle-luzern.ch");
    expect(m.orgId).toBe("8bfec2f4-6184-40b7-b1aa-c8933d506a87");
    expect(m.brandIds).toEqual(["f2408cfb-4f02-4910-acec-e61fc8edb9cf"]);
    expect(m.text).toContain("Schweiz");
    expect(m.threadKey).toBe("<5ac53340-e14c-ac34-7685-52a54368fbe2@marketingagency.group>");
  });

  it("an IMAP message on no thread of ours carries no attribution", () => {
    const m = buildImapMessage({ ...MARKUS_IMAP_ROW, kind: "unrelated", instantly_campaign_id: null });
    expect(m.kind).toBe("unrelated");
    expect(m.leadEmail).toBeNull();
    expect(m.orgId).toBeNull();
    expect(m.from?.address).toBe("markus@markthalle-luzern.ch");
  });

  it("the reply fact names Markus as `from`, the fact stays on Thomas", () => {
    const out = factEnvelope(
      "reply",
      {
        lead_email: "thomas@markthalle-luzern.ch",
        env_imap_account: MARKUS_IMAP_ROW.account_email,
        env_imap_from: MARKUS_IMAP_ROW.from_address,
        env_imap_message_id: MARKUS_IMAP_ROW.message_id,
        env_imap_payload: MARKUS_IMAP_ROW.payload,
      },
      {},
    );
    expect(out.participants?.find((p) => p.role === "from")?.email).toBe("markus@markthalle-luzern.ch");
    expect(out.threadKey).toBe("<5ac53340-e14c-ac34-7685-52a54368fbe2@marketingagency.group>");
  });

  it("a sequence step fact: our mailbox to the lead alone", () => {
    const out = factEnvelope(
      "email_sent",
      { lead_email: "thomas@markthalle-luzern.ch", env_step_message_id: "<5ac@x>", env_step_payload: {} },
      { accountEmail: "kevin@marketingagency.group" },
    );
    expect(out.participants).toEqual([
      { email: "kevin@marketingagency.group", role: "from" },
      { email: "thomas@markthalle-luzern.ch", role: "to" },
    ]);
    expect(out.threadKey).toBe("<5ac@x>");
  });

  it("other fact types carry neither field", () => {
    expect(factEnvelope("email_bounced", {}, {})).toEqual({});
  });
});

describe("what we sent states every address on it", () => {
  it("today's answer (To = the lead, agency in Cc) is read from the dispatch log as stored", () => {
    const env = dispatchEnvelope({
      accountEmail: "kevin@marketingagency.group",
      leadEmail: "thomas@markthalle-luzern.ch",
      messageId: "<043d@marketingagency.group>",
      payload: {
        cc: "kevin@distribute.you",
        inReplyTo: "<CAE@mail.gmail.com>",
        references: ["<5ac@marketingagency.group>", "<CAE@mail.gmail.com>"],
      },
    });
    expect(env.participants).toEqual([
      { email: "kevin@marketingagency.group", role: "from" },
      { email: "thomas@markthalle-luzern.ch", role: "to" },
      { email: "kevin@distribute.you", role: "cc" },
    ]);
    expect(env.threadKey).toBe("<5ac@marketingagency.group>");
  });

  it("a caller-chosen To / Bcc on the row wins over the lead", () => {
    const m = buildDispatchMessage({
      id: "d1",
      instantly_campaign_id: "self:x",
      lead_email: "thomas@markthalle-luzern.ch",
      account_email: "kevin@marketingagency.group",
      step: 0,
      outcome: "sent",
      message_id: "<ours@x>",
      payload: {
        to: "markus@markthalle-luzern.ch",
        cc: "thomas@markthalle-luzern.ch",
        bcc: "kevin@distribute.you",
        sentBy: "automation",
        subject: "Re: Flohsamenschalen",
        bodyHtml: "<p>Ja, aus der Schweiz.</p>",
      },
      dispatched_at: new Date("2026-11-09T16:00:00Z"),
      a_email: null,
    });
    expect(m.kind).toBe("manual_reply");
    expect(m.sentBy).toBe("automation");
    expect(m.to.map((a) => a.address)).toEqual(["markus@markthalle-luzern.ch"]);
    expect(m.cc.map((a) => a.address)).toEqual(["thomas@markthalle-luzern.ch"]);
    expect(m.bcc.map((a) => a.address)).toEqual(["kevin@distribute.you"]);
    expect(m.text).toContain("Schweiz");
  });

  it("the Instantly mirror's lists are read too", () => {
    const env = instantlyEnvelope({
      from_address_email: "drgallego@shinwellness.com",
      to_address_email_list: "amy@boostdistribute.com",
      cc_address_email_list: "kevin@distribute.you,dev@docdinners.com",
      message_id: "<m@x>",
    });
    expect(env.participants.map((p) => `${p.role}:${p.email}`)).toEqual([
      "from:drgallego@shinwellness.com",
      "to:amy@boostdistribute.com",
      "cc:kevin@distribute.you",
      "cc:dev@docdinners.com",
    ]);
  });
});
