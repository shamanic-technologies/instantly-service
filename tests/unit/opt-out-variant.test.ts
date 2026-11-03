import { describe, it, expect } from "vitest";

import {
  DEFAULT_OPT_OUT_VARIANT,
  isMailtoUnsubscribeSubject,
  mailtoUnsubscribeSends,
  optOutHeaders,
  optOutTarget,
} from "../../src/lib/self-send/opt-out-variant";

describe("opt-out variants (UNSUB-3WAY-1103)", () => {
  it("prospect mail defaults to the sending mailbox, no outside domain (owner go 2026-11-03)", () => {
    expect(DEFAULT_OPT_OUT_VARIANT).toBe("mailbox_only");
  });

  it("each variant's header", () => {
    const tracker = optOutTarget("tracker_link", { trackerUrl: () => "https://l.test/u/a/b", senderEmail: "amy@one.com" });
    expect(optOutHeaders("tracker_link", tracker)).toEqual({
      "List-Unsubscribe": "<https://l.test/u/a/b>",
      "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    });
    const mailbox = optOutTarget("mailbox_only", { trackerUrl: () => { throw new Error("never minted"); }, senderEmail: "amy@one.com" });
    expect(optOutHeaders("mailbox_only", mailbox)).toEqual({ "List-Unsubscribe": "<mailto:amy@one.com?subject=unsubscribe>" });
    expect(optOutHeaders("none", optOutTarget("none", { trackerUrl: () => "x", senderEmail: "amy@one.com" }))).toEqual({});
  });
});

describe("a mailto unsubscribe email stops the person (UNSUB-3WAY-1103)", () => {
  const sends = [
    { instantlyCampaignId: "self:a", leadEmail: "Jane@Acme.com", step: 1 },
    { instantlyCampaignId: "self:a", leadEmail: "jane@acme.com", step: 2 },
    { instantlyCampaignId: "self:b", leadEmail: "jane@acme.com", step: 1 },
    { instantlyCampaignId: "self:c", leadEmail: "bob@acme.com", step: 3 },
  ];

  it("recognises the subject the button sends", () => {
    expect(isMailtoUnsubscribeSubject("unsubscribe")).toBe(true);
    expect(isMailtoUnsubscribeSubject(" Unsubscribe ")).toBe(true);
    expect(isMailtoUnsubscribeSubject("Re: Quick question")).toBe(false);
    expect(isMailtoUnsubscribeSubject("please unsubscribe me")).toBe(false);
    expect(isMailtoUnsubscribeSubject(null)).toBe(false);
  });

  it("stops every sequence this mailbox sent to the sender's exact address, latest step each", () => {
    expect(mailtoUnsubscribeSends("jane@acme.com", sends)).toEqual([
      { instantlyCampaignId: "self:a", leadEmail: "jane@acme.com", step: 2 },
      { instantlyCampaignId: "self:b", leadEmail: "jane@acme.com", step: 1 },
    ]);
  });

  it("stops nobody for an address this mailbox never wrote to", () => {
    expect(mailtoUnsubscribeSends("colleague@acme.com", sends)).toEqual([]);
    expect(mailtoUnsubscribeSends(null, sends)).toEqual([]);
  });
});
