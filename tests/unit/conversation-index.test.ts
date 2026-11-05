import { describe, it, expect } from "vitest";

import {
  conversationMessageId,
  pickConversationMessages,
  previewOf,
  toConversationIndex,
  PREVIEW_MAX_CHARS,
} from "../../src/lib/conversation-index";
import type {
  ConversationMessage,
  LeadConversation,
} from "../../src/lib/lead-conversation";

function msg(
  direction: "inbound" | "outbound",
  at: string,
  text: string,
  extra: Partial<ConversationMessage> = {},
): ConversationMessage {
  return {
    direction,
    from: direction === "outbound" ? "amy@ours.com" : "edmund@prospect.com",
    to: direction === "outbound" ? "edmund@prospect.com" : "amy@ours.com",
    at,
    subject: "Re: intro",
    text,
    campaignId: "camp-1",
    instantlyCampaignId: "self:1",
    outreachFact: null,
    ...extra,
  };
}

// Edmund Bartlett's shape (2026-11-04): our cold email, his positive reply, then
// THREE answers of ours that a front-truncated full read hid from Sophie.
const LONG_ANSWER = "Thanks Edmund. ".repeat(2000);
const EDMUND: LeadConversation = {
  campaignId: "camp-1",
  campaignIds: ["camp-1"],
  instantlyCampaignId: "self:1",
  leadEmail: "edmund@prospect.com",
  accountEmail: "amy@ours.com",
  transport: "smtp",
  source: "self_send",
  messageCount: 5,
  messages: [
    msg("outbound", "2026-10-20T09:00:00.000Z", "Hi Edmund, quick idea for Beyond Logic."),
    msg("inbound", "2026-10-21T10:00:00.000Z", "Sounds interesting, tell me more.\n\nOn Mon, Oct 20, 2026 Amy wrote:\n> Hi Edmund"),
    msg("outbound", "2026-10-21T11:00:00.000Z", LONG_ANSWER),
    msg("outbound", "2026-10-23T11:00:00.000Z", "Following up on my last note."),
    msg("outbound", "2026-10-28T11:00:00.000Z", "Here is a time to talk.", {
      attachments: [{ name: "offer.pdf" } as never],
    }),
  ],
  sequences: [],
};

describe("toConversationIndex", () => {
  it("lists EVERY message, newest first, our answers after his reply included", () => {
    const index = toConversationIndex(EDMUND);
    expect(index.order).toBe("newest_first");
    expect(index.messages).toHaveLength(5);
    expect(index.messages.map((m) => m.at)).toEqual([
      "2026-10-28T11:00:00.000Z",
      "2026-10-23T11:00:00.000Z",
      "2026-10-21T11:00:00.000Z",
      "2026-10-21T10:00:00.000Z",
      "2026-10-20T09:00:00.000Z",
    ]);
    expect(index.inboundCount).toBe(1);
    expect(index.outboundCount).toBe(4);
    expect(index.lastInboundAt).toBe("2026-10-21T10:00:00.000Z");
    expect(index.lastOutboundAt).toBe("2026-10-28T11:00:00.000Z");
    expect(index.messages[0].hasAttachments).toBe(true);
  });

  it("stays small whatever the bodies weigh (a 30k-char answer is one short line)", () => {
    const index = toConversationIndex(EDMUND);
    const long = index.messages.find((m) => m.chars === LONG_ANSWER.length)!;
    expect(long.preview.length).toBeLessThanOrEqual(PREVIEW_MAX_CHARS);
    expect(JSON.stringify(index).length).toBeLessThan(3000);
    expect(JSON.stringify(index)).not.toContain("text");
  });

  it("keeps undated messages last", () => {
    const c = { ...EDMUND, messages: [msg("inbound", "", "no date"), ...EDMUND.messages] };
    const index = toConversationIndex(c);
    expect(index.messages[index.messages.length - 1].preview).toBe("no date");
  });
});

describe("previewOf", () => {
  it("cuts the quoted history and folds lines", () => {
    expect(previewOf("Sounds good.\nCall me.\n\nOn Mon, Oct 20, 2026 Amy wrote:\n> old")).toBe(
      "Sounds good. Call me.",
    );
    expect(previewOf("Thanks.\n\nOn Mon, Nov 2, 2026 at 8:24 AM Kevin Lourd\nwrote:\n> x")).toBe("Thanks.");
    expect(previewOf("Oui\nLe lun. 20 oct. 2026, Amy a écrit :\n> x")).toBe("Oui");
  });
  it("a message that is only a quote still previews its words", () => {
    expect(previewOf("> only quoted")).toBe("> only quoted");
  });
});

describe("conversationMessageId / pickConversationMessages", () => {
  it("is stable across reads and distinct per message", () => {
    const a = toConversationIndex(EDMUND).messages.map((m) => m.id);
    const b = toConversationIndex(structuredClone(EDMUND)).messages.map((m) => m.id);
    expect(a).toEqual(b);
    expect(new Set(a).size).toBe(5);
    expect(a[0]).toMatch(/^msg_[0-9a-f]{20}$/);
  });

  it("returns asked messages in full, in the order asked, and names unknown ids", () => {
    const ids = EDMUND.messages.map(conversationMessageId);
    const picked = pickConversationMessages(EDMUND, [ids[2], ids[0], "msg_nope"]);
    expect(picked.messages.map((m) => m.text)).toEqual([LONG_ANSWER, EDMUND.messages[0].text]);
    expect(picked.messages[0].id).toBe(ids[2]);
    expect(picked.missing).toEqual(["msg_nope"]);
  });
});

