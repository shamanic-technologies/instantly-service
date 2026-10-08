import { describe, it, expect } from "vitest";

import {
  buildOrphanReplyQuestion,
  ORPHAN_REPLY_NONE,
  orphanReplyExclusion,
  rankOrphanReplyLeads,
  readOrphanReplyVerdict,
  type OrphanReplyLead,
} from "../../src/lib/self-send/orphan-reply";

const OWN = new Set(["saviolabsco.com", "salesmolt.com"]);

function lead(overrides: Partial<OrphanReplyLead> = {}): OrphanReplyLead {
  return {
    instantlyCampaignId: "self:stacy",
    leadEmail: "stacy.blecher@twinhealth.com",
    firstName: "Stacy",
    lastName: "Blecher",
    companyName: "Twin Health",
    subject: "Growing a holistic practice in Charleston",
    excerpt: "Hi Stacy, Doc Dinners hosts physician dinners in Charleston.",
    lastStep: 1,
    firstSentAt: new Date("2026-09-24T12:54:44Z"),
    lastSentAt: new Date("2026-09-24T12:54:44Z"),
    orgId: "org-1",
    ...overrides,
  };
}

/**
 * Stage 1: what can NEVER be a prospect's answer and must never reach a paid
 * judgment. Every reason is about who sent it or how, never what it says.
 */
describe("orphanReplyExclusion", () => {
  const human = {
    fromAddress: '"Stacy Blecher" <drblecher@chsmetabolismdoc.com>',
    subject: "Doc Dinners",
    headers: {},
  };

  it("lets a person writing from an address we never emailed through", () => {
    expect(orphanReplyExclusion(human, OWN)).toBeNull();
  });

  it("lets a small practice's info@ through — that is how they answer", () => {
    expect(
      orphanReplyExclusion({ ...human, fromAddress: "Front desk <info@chsmetabolismdoc.com>" }, OWN),
    ).toBeNull();
  });

  it("excludes our own fleet's warmup mesh and our staff", () => {
    expect(orphanReplyExclusion({ ...human, fromAddress: "eric@salesmolt.com" }, OWN)).toBe("own_domain");
    expect(orphanReplyExclusion({ ...human, fromAddress: "kevin@distribute.you" }, OWN)).toBe("staff");
  });

  it("excludes Instantly's warmup pool by its workspace tag, in the subject or the body", () => {
    expect(
      orphanReplyExclusion({ ...human, subject: "Kevin - coffee? | RXYQDJD WNT6JJB" }, OWN),
    ).toBe("instantly_warmup");
    expect(
      orphanReplyExclusion({ ...human, subject: "Re: hi", text: "see you D4B8AFUALOMLN3WWNT6JJBI8CC7" }, OWN),
    ).toBe("instantly_warmup");
  });

  it("excludes lists, autoresponders, DSNs and machine senders", () => {
    expect(orphanReplyExclusion({ ...human, headers: { "list-unsubscribe": "<x>" } }, OWN)).toBe("mailing_list");
    expect(orphanReplyExclusion({ ...human, headers: { list: "{}" } }, OWN)).toBe("mailing_list");
    expect(orphanReplyExclusion({ ...human, headers: { "auto-submitted": "auto-replied" } }, OWN)).toBe("automated");
    expect(
      orphanReplyExclusion(
        { ...human, headers: { "content-type": "multipart/report; report-type=delivery-status" } },
        OWN,
      ),
    ).toBe("automated");
    expect(orphanReplyExclusion({ ...human, fromAddress: "noreply-dmarc-support@google.com" }, OWN)).toBe(
      "system_sender",
    );
    expect(orphanReplyExclusion({ ...human, fromAddress: null, headers: {} }, OWN)).toBe("no_sender");
  });
});

/** Stage 2: which leads the judgment is offered. A pre-filter, never a verdict. */
describe("rankOrphanReplyLeads", () => {
  const stacyAnswer = {
    fromAddress: '"Stacy Blecher" <drblecher@chsmetabolismdoc.com>',
    subject: "Doc Dinners",
    text: "Hi Michaela, yes I would be interested.",
    receivedAt: new Date("2026-09-24T14:56:55Z"),
  };

  it("offers the lead whose name the stranger's address carries (the Doc Dinners case)", () => {
    const other = lead({
      instantlyCampaignId: "self:other",
      leadEmail: "john@clinic.com",
      firstName: "John",
      lastName: "Doe",
      companyName: "Clinic",
      subject: "Dinners for Austin doctors",
    });
    const ranked = rankOrphanReplyLeads(stacyAnswer, [other, lead()]);
    expect(ranked.map((r) => r.lead.instantlyCampaignId)).toEqual(["self:stacy"]);
  });

  it("offers the lead a COLLEAGUE answers for, through the quoted pitch and company domain", () => {
    const ranked = rankOrphanReplyLeads(
      {
        fromAddress: "Jane Roe <jane@twinhealth.com>",
        subject: "Re: Fwd: Growing a holistic practice in Charleston",
        text: "Stacy passed this along, can we talk?",
        receivedAt: new Date("2026-09-26T10:00:00Z"),
      },
      [lead()],
    );
    expect(ranked).toHaveLength(1);
  });

  it("never offers a lead we had not yet written to when the message arrived", () => {
    expect(
      rankOrphanReplyLeads(stacyAnswer, [lead({ firstSentAt: new Date("2026-09-25T00:00:00Z") })]),
    ).toEqual([]);
  });

  it("offers nothing for mail sharing nothing with any lead (no judgment is paid)", () => {
    expect(
      rankOrphanReplyLeads(
        {
          fromAddress: '"Healthy Gums Routine" <support@hammersound.net>',
          subject: "The soft dental chocolate that rebuilds teeth",
          text: "Buy now",
          receivedAt: new Date("2026-09-30T00:00:00Z"),
        },
        [lead()],
      ),
    ).toEqual([]);
  });
});

/** Stage 3: the judgment is the verdict, and only a confident pick acts. */
describe("orphan-reply judgment", () => {
  const ranked = [{ lead: lead(), score: 5 }];

  it("asks a choice between every offered lead and none", () => {
    const question = buildOrphanReplyQuestion(ranked);
    expect(question.type).toBe("choice");
    expect(Object.keys(question.criteria)).toEqual(["lead_1", ORPHAN_REPLY_NONE]);
    expect(JSON.stringify(question.criteria.lead_1)).toContain("stacy.blecher@twinhealth.com");
  });

  it("matches only a lead picked at or above the bar", () => {
    expect(
      readOrphanReplyVerdict(
        { type: "choice", choice: "lead_1", confidence: 0.9, probabilities: { lead_1: 0.93, none: 0.07 } },
        ranked,
      ),
    ).toMatchObject({ outcome: "matched", probability: 0.93 });
    expect(
      readOrphanReplyVerdict(
        { type: "choice", choice: "lead_1", confidence: 0.2, probabilities: { lead_1: 0.55, none: 0.45 } },
        ranked,
      ).outcome,
    ).toBe("low_confidence");
    expect(
      readOrphanReplyVerdict(
        { type: "choice", choice: "none", confidence: 0.9, probabilities: { lead_1: 0.05, none: 0.95 } },
        ranked,
      ).outcome,
    ).toBe("none");
  });

  it("fails loud on an option it never offered rather than guessing", () => {
    expect(() =>
      readOrphanReplyVerdict(
        { type: "choice", choice: "lead_7", confidence: 1, probabilities: { lead_7: 1 } },
        ranked,
      ),
    ).toThrow(/unknown option/);
  });
});
