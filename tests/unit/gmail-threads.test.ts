import { describe, it, expect } from "vitest";

import {
  chooseSequence,
  leadMessageKind,
  planExchange,
  type SequenceCandidate,
} from "../../src/lib/self-send/gmail-threads";
import type { MirroredMessage } from "../../src/lib/google-client";
import { isStaffSender } from "../../src/lib/staff-senders";

// STAFF-GMAIL-THREAD-1013, the real shape: Kevin answered Lukas from his own
// Gmail on 2026-10-11 (In-Reply-To = a Postmark id, nothing of our sends), and
// Lukas wrote back to Kevin on 2026-10-13.
const LEAD = "lukas@stankamera.com";
const leads = new Set([LEAD, "other@lead.com"]);
const isOurs = (a: string): boolean => isStaffSender(a);

function msg(extra: Partial<MirroredMessage> = {}): MirroredMessage {
  return {
    mirrorId: "kevin.lourd@gmail.com:g1",
    gmailMessageId: "g1",
    messageId: "<CAJNV@mail.gmail.com>",
    inReplyTo: "<CAF9A@mail.gmail.com>",
    references: "<46c4e5ef@mtasv.net> <CAF9A@mail.gmail.com>",
    headers: {},
    from: LEAD,
    to: ["kevin@distribute.you"],
    cc: [],
    subject: "Re: Following up",
    date: new Date("2026-10-13T15:24:38Z"),
    text: "Hi Kevin,\n\nSounds good; let's set some time to chat.",
    ...extra,
  };
}

describe("planExchange: which messages are a lead <-> us conversation", () => {
  it("the lead writing to one of our people is the lead's message", () => {
    expect(planExchange(msg(), leads, isOurs)).toEqual([{ leadEmail: LEAD, side: "lead" }]);
  });

  it("one of our people writing to the lead is our side", () => {
    const answer = msg({ from: "kevin@distribute.you", to: [LEAD] });
    expect(planExchange(answer, leads, isOurs)).toEqual([{ leadEmail: LEAD, side: "ours" }]);
  });

  it("the owner's personal Gmail counts as ours too", () => {
    const answer = msg({ from: "kevin.lourd@gmail.com", to: [], cc: [LEAD] });
    expect(planExchange(answer, leads, isOurs)).toEqual([{ leadEmail: LEAD, side: "ours" }]);
  });

  it("the lead writing to a sending alias is the poll's, not an exchange with us", () => {
    expect(planExchange(msg({ to: ["allie@leansignalio.com"] }), leads, isOurs)).toEqual([]);
  });

  it("someone we hold no sequence for is not read", () => {
    expect(planExchange(msg({ from: "friend@example.com" }), leads, isOurs)).toEqual([]);
    expect(planExchange(msg({ from: "kevin@distribute.you", to: ["friend@example.com"] }), leads, isOurs)).toEqual([]);
  });

  it("a display-name From still resolves to the address", () => {
    const m = msg({ from: "Lukas Stankevicius <Lukas@Stankamera.com>" });
    expect(planExchange(m, leads, isOurs)).toEqual([{ leadEmail: LEAD, side: "lead" }]);
  });
});

describe("leadMessageKind", () => {
  it("a human answer is a reply", () => {
    expect(leadMessageKind(msg())).toBe("reply");
  });
  it("an out-of-office is an auto-reply, never a reply (it would stop the sequence)", () => {
    expect(leadMessageKind(msg({ headers: { "auto-submitted": "auto-replied" } }))).toBe("auto_reply");
  });
  it("a delivery report is not the lead's message", () => {
    expect(leadMessageKind(msg({ headers: { "content-type": "multipart/report; report-type=delivery-status" } }))).toBeNull();
  });
});

function seq(extra: Partial<SequenceCandidate>): SequenceCandidate {
  return {
    instantlyCampaignId: "self:a",
    orgId: "org-1",
    accountEmail: "allie@leansignalio.com",
    step: 3,
    lastSentAt: new Date("2026-10-06T14:56:17Z"),
    replied: false,
    ...extra,
  };
}

describe("chooseSequence", () => {
  it("no sequence had emailed them by then: none", () => {
    expect(chooseSequence([], new Set())).toEqual({ outcome: "none" });
  });

  it("one sequence: that one (Lukas)", () => {
    const only = seq({ replied: true });
    expect(chooseSequence([only], new Set())).toEqual({ outcome: "matched", sequence: only });
  });

  it("one org: the sequence they replied to wins over a later one they did not", () => {
    const replied = seq({ instantlyCampaignId: "self:a", replied: true });
    const later = seq({ instantlyCampaignId: "self:b", lastSentAt: new Date("2026-10-10T00:00:00Z") });
    expect(chooseSequence([later, replied], new Set())).toEqual({ outcome: "matched", sequence: replied });
  });

  it("one org, none replied: the latest emailed", () => {
    const early = seq({ instantlyCampaignId: "self:a" });
    const late = seq({ instantlyCampaignId: "self:b", lastSentAt: new Date("2026-10-10T00:00:00Z") });
    expect(chooseSequence([early, late], new Set())).toEqual({ outcome: "matched", sequence: late });
  });

  it("two orgs and nothing names one: declined, never filed under another client's brand", () => {
    const a = seq({ instantlyCampaignId: "self:a", orgId: "org-1" });
    const b = seq({ instantlyCampaignId: "self:b", orgId: "org-2" });
    expect(chooseSequence([a, b], new Set())).toEqual({ outcome: "ambiguous" });
  });

  it("two orgs, a reference to one of our sends names it", () => {
    const a = seq({ instantlyCampaignId: "self:a", orgId: "org-1" });
    const b = seq({ instantlyCampaignId: "self:b", orgId: "org-2" });
    expect(chooseSequence([a, b], new Set(["self:b"]))).toEqual({ outcome: "matched", sequence: b });
  });
});
