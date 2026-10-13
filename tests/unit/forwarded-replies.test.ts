import { describe, it, expect, vi } from "vitest";

vi.mock("../../src/db", () => ({ db: {} }));

import {
  groupTargetMailboxes,
  isDeliveredLate,
  planMirroredMessage,
  type AliasedSend,
} from "../../src/lib/self-send/forwarded-replies";
import type { MirroredMessage } from "../../src/lib/google-client";

const OUR_ID = "<abc@pressbeat.ai>";
const OTHER_ID = "<zzz@pressbeat.ai>";

const lauraSend: AliasedSend = {
  instantlyCampaignId: "22cf0ff1",
  leadEmail: "laura@fusionmedcare.com",
  step: 1,
  orgId: "org-1",
  accountEmail: "klourd@pressbeat.ai",
};
const otherSend: AliasedSend = { ...lauraSend, instantlyCampaignId: "other", leadEmail: "x@y.com", accountEmail: "kevin@pressbeat.ai" };
const known = new Map<string, AliasedSend>([
  [OUR_ID, lauraSend],
  [OTHER_ID, otherSend],
]);
const aliases = new Set(["kevin@pressbeat.ai", "klourd@pressbeat.ai", "lourd@pressbeat.ai"]);

function msg(extra: Partial<MirroredMessage> = {}): MirroredMessage {
  return {
    mirrorId: "g1",
    messageId: "<laura-1@fusionmedcare.com>",
    inReplyTo: OUR_ID,
    references: OUR_ID,
    headers: {},
    from: "laura@fusionmedcare.com",
    to: ["klourd@pressbeat.ai"],
    cc: [],
    subject: "Re: Shockwave equipment costs vs patient outcomes",
    date: new Date("2026-08-18T16:42:16Z"),
    text: "Open to learn more",
    ...extra,
  };
}

describe("planMirroredMessage: the poll's own correlation, on a forwarded copy", () => {
  it("Laura's reply threads onto our send: filed as her reply under the alias that sent it", () => {
    const plan = planMirroredMessage(msg(), aliases, known);
    expect(plan).toMatchObject({ action: "file", kind: "reply", send: { accountEmail: "klourd@pressbeat.ai", leadEmail: "laura@fusionmedcare.com" } });
  });

  it("an autoresponder is an auto_reply, never a reply", () => {
    const plan = planMirroredMessage(msg({ headers: { "Auto-Submitted": "auto-replied" } }), aliases, known);
    expect(plan).toMatchObject({ action: "file", kind: "auto_reply" });
  });

  it("mail referencing none of our sends (warmup, newsletters) is skipped", () => {
    expect(planMirroredMessage(msg({ inReplyTo: "<x@else>", references: null }), aliases, known)).toEqual({ action: "skip", reason: "unrelated" });
  });

  it("references reaching two different sequences are ambiguous: declined", () => {
    expect(planMirroredMessage(msg({ references: `${OUR_ID} ${OTHER_ID}` }), aliases, known)).toEqual({ action: "skip", reason: "ambiguous" });
  });

  it("no Message-ID, nothing to dedup on: skipped", () => {
    expect(planMirroredMessage(msg({ messageId: null }), aliases, known)).toEqual({ action: "skip", reason: "no_message_id" });
  });

  it("our own answer sent from Gmail AS the alias, to the lead: filed staff_reply", () => {
    const plan = planMirroredMessage(
      msg({ from: "klourd@pressbeat.ai", to: ["laura@fusionmedcare.com"], inReplyTo: "<laura-1@fusionmedcare.com>", references: `${OUR_ID} <laura-1@fusionmedcare.com>` }),
      aliases,
      known,
    );
    expect(plan).toMatchObject({ action: "file", kind: "staff_reply" });
  });

  it("our alias writing on the thread to someone else is not an answer to the lead", () => {
    const plan = planMirroredMessage(msg({ from: "klourd@pressbeat.ai", to: ["someone@else.com"] }), aliases, known);
    expect(plan).toEqual({ action: "skip", reason: "not_to_the_lead" });
  });
});

describe("isDeliveredLate", () => {
  const asOf = new Date("2026-10-13T12:00:00Z");
  it("a reply from August is late", () => expect(isDeliveredLate(new Date("2026-08-18T16:42:16Z"), asOf)).toBe(true));
  it("a reply from this morning is not", () => expect(isDeliveredLate(new Date("2026-10-13T08:00:00Z"), asOf)).toBe(false));
  it("an undated message is treated as late", () => expect(isDeliveredLate(null, asOf)).toBe(true));
});

describe("groupTargetMailboxes", () => {
  const logins = new Map([
    ["kevin@pressbeat.ai", "kevin@pressbeat.ai"],
    ["klourd@pressbeat.ai", "kevin@pressbeat.ai"],
    ["kevin@growthagency.ch", "kevin@growthagency.ch"],
    ["lourd@growthagency.ch", "kevin@growthagency.ch"],
    ["klourd@growthagency.ch", "kevin@growthagency.ch"],
  ]);
  it("a domain selects every alias of its mailbox", () => {
    const g = groupTargetMailboxes(logins, { mailboxes: new Set(), domains: new Set(["pressbeat.ai"]) });
    expect([...g.keys()]).toEqual(["kevin@pressbeat.ai"]);
    expect([...g.get("kevin@pressbeat.ai")!].sort()).toEqual(["kevin@pressbeat.ai", "klourd@pressbeat.ai"]);
  });
  it("a named alias selects its whole mailbox", () => {
    const g = groupTargetMailboxes(logins, { mailboxes: new Set(["lourd@growthagency.ch"]), domains: new Set() });
    expect([...g.get("kevin@growthagency.ch")!].sort()).toEqual(["kevin@growthagency.ch", "klourd@growthagency.ch", "lourd@growthagency.ch"]);
  });
});

import { toMirroredMessage } from "../../src/lib/google-client";

describe("toMirroredMessage: google-service's wire shape", () => {
  it("Laura as served in prod: threading ids kept, references joined, automation as headers", () => {
    const m = toMirroredMessage({
      id: "kevin.lourd@gmail.com:1a015c0c787d3b13",
      messageId: "<CAEnT=oOCULuUVVQyZgVH5NYs_ZzqH95fGWbDP3pCyKqcBpkPJA@mail.gmail.com>",
      inReplyTo: "<01a01579-f1ef-7824-a890-a313949037bf@pressbeat.ai>",
      references: ["<01a01579-f1ef-7824-a890-a313949037bf@pressbeat.ai>"],
      fromEmail: "laura@fusionmedcare.com",
      to: ["klourd@pressbeat.ai"],
      cc: [],
      subject: "Re: Shockwave equipment costs vs patient outcomes",
      sentAt: "2026-08-18T16:42:16Z",
      automation: { autoSubmitted: null, precedence: "bulk" },
      mimeType: "multipart/alternative",
      bodyText: "Open to learn more",
    });
    expect(m.references).toBe("<01a01579-f1ef-7824-a890-a313949037bf@pressbeat.ai>");
    expect(m.headers).toEqual({ precedence: "bulk", "content-type": "multipart/alternative" });
    expect(m.date?.toISOString()).toBe("2026-08-18T16:42:16.000Z");
  });
});
