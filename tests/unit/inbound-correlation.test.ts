/**
 * Correlating an inbound message to the sequence it answers.
 *
 * The interesting cases are the ones where a naive implementation is confidently
 * wrong: a reply to an Instantly-sent sequence (no dispatch row of ours), a
 * reply from an address we never wrote to, and a message two sequences could
 * both claim.
 */

import { describe, it, expect } from "vitest";

import {
  classifyInbound,
  correlateSend,
  type CorrelatedSend,
} from "../../src/lib/self-send/inbound";

const OURS = "<01a01f7d-b321-7a6b-a454-f88a21be322f@marketingagency.network>";
const OURS_STEP2 = "<01a04387-a39a-7149-80b1-3e9eda06288c@marketingagency.network>";
const OTHER_SEQUENCE = "<other-0001@marketingagency.network>";

function sends(
  entries: Array<[string, CorrelatedSend]>,
): Map<string, CorrelatedSend> {
  return new Map(entries);
}

const JASON: CorrelatedSend = {
  instantlyCampaignId: "e1e216ca-635a-4682-92be-f5057f8224ea",
  leadEmail: "jason@uhmedical.com",
  step: 1,
};

const JASON_STEP2: CorrelatedSend = { ...JASON, step: 2 };

const SOMEBODY_ELSE: CorrelatedSend = {
  instantlyCampaignId: "90097300-e993-4c43-9cb9-f54a34fcc004",
  leadEmail: "filippo@bewesrl.com",
  step: 2,
};

describe("correlateSend — exact-key matching", () => {
  it("matches a reply to a send Instantly made, not just one of ours", () => {
    // The whole point of the widening: this Message-Id came out of Instantly's
    // own Unibox mirror, and there is no smtp_dispatch_raw row for it at all.
    const result = correlateSend([OURS], sends([[OURS, JASON]]));

    expect(result).toEqual({ outcome: "matched", send: JASON });
  });

  it("returns `none` when nothing we sent is referenced", () => {
    const result = correlateSend(
      ["<a-stranger@example.com>"],
      sends([[OURS, JASON]]),
    );

    expect(result).toEqual({ outcome: "none" });
  });

  it("returns `none` on an empty reference list", () => {
    expect(correlateSend([], sends([[OURS, JASON]]))).toEqual({ outcome: "none" });
  });
});

describe("correlateSend — one sequence named several times is NOT ambiguous", () => {
  it("collapses a full References thread onto its single sequence", () => {
    // `References` carries the whole thread, so a reply to step 2 legitimately
    // names step 1 as well. Two ids, one sequence — nothing ambiguous about it.
    const result = correlateSend(
      [OURS_STEP2, OURS],
      sends([
        [OURS, JASON],
        [OURS_STEP2, JASON_STEP2],
      ]),
    );

    expect(result.outcome).toBe("matched");
  });

  it("attributes it to the LATEST step named, the email they were looking at", () => {
    const result = correlateSend(
      [OURS, OURS_STEP2],
      sends([
        [OURS, JASON],
        [OURS_STEP2, JASON_STEP2],
      ]),
    );

    expect(result).toEqual({ outcome: "matched", send: JASON_STEP2 });
  });
});

describe("correlateSend — ambiguity is declined, never guessed", () => {
  it("refuses to pick when the references reach two different sequences", () => {
    const result = correlateSend(
      [OURS, OTHER_SEQUENCE],
      sends([
        [OURS, JASON],
        [OTHER_SEQUENCE, SOMEBODY_ELSE],
      ]),
    );

    expect(result.outcome).toBe("ambiguous");
  });

  it("names both campaigns so the log can say which two", () => {
    const result = correlateSend(
      [OURS, OTHER_SEQUENCE],
      sends([
        [OURS, JASON],
        [OTHER_SEQUENCE, SOMEBODY_ELSE],
      ]),
    );

    if (result.outcome !== "ambiguous") throw new Error("expected ambiguous");
    expect(result.campaignIds.sort()).toEqual(
      [JASON.instantlyCampaignId, SOMEBODY_ELSE.instantlyCampaignId].sort(),
    );
  });

  it("does not fall back to the first match — the caller gets no `send`", () => {
    const result = correlateSend(
      [OURS, OTHER_SEQUENCE],
      sends([
        [OURS, JASON],
        [OTHER_SEQUENCE, SOMEBODY_ELSE],
      ]),
    );

    expect(result).not.toHaveProperty("send");
  });
});

describe("correlateSend — the sender is never consulted", () => {
  it("correlates a reply sent from an address we never wrote to", () => {
    // A prospect routinely answers from an assistant's mailbox or a shared
    // partners@ box. Matching the sender against the lead would drop exactly
    // these; the Message-Id does not care who typed it.
    const headers = {
      "in-reply-to": OURS,
      from: "assistant@uhmedical.com",
    };

    const classification = classifyInbound(headers, "", new Set([OURS]));
    const result = correlateSend(
      classification.referencedMessageIds,
      sends([[OURS, JASON]]),
    );

    expect(classification.kind).toBe("reply");
    expect(result).toEqual({ outcome: "matched", send: JASON });
    // The lead stays the one on the campaign row, not the address that wrote.
    if (result.outcome !== "matched") throw new Error("expected match");
    expect(result.send.leadEmail).toBe("jason@uhmedical.com");
  });

  it("still does NOT correlate on the address alone when no id matches", () => {
    // Same prospect, no thread reference: absence of a match is honest.
    const classification = classifyInbound(
      { from: "jason@uhmedical.com" },
      "",
      new Set([OURS]),
    );

    expect(classification.kind).toBe("unrelated");
    expect(
      correlateSend(classification.referencedMessageIds, sends([[OURS, JASON]])),
    ).toEqual({ outcome: "none" });
  });
});

describe("correlateSend — Jason's exact shape, end to end through the classifier", () => {
  it("classifies as a reply and correlates to his Instantly-sent sequence", () => {
    const headers = {
      "in-reply-to": OURS_STEP2,
      references: `${OURS} ${OURS_STEP2}`,
      from: "Jason Leavitt <jason@uhmedical.com>",
      subject: "Re: dinner talks",
    };
    const body =
      "I now give dinner talks. I would be interested. Can you send me the costs and details.";

    const known = sends([
      [OURS, JASON],
      [OURS_STEP2, JASON_STEP2],
    ]);
    const classification = classifyInbound(headers, body, new Set(known.keys()));
    const result = correlateSend(classification.referencedMessageIds, known);

    expect(classification.kind).toBe("reply");
    expect(result).toEqual({ outcome: "matched", send: JASON_STEP2 });
  });

  it("an out-of-office on the same thread classifies as auto_reply, not reply", () => {
    const headers = {
      "in-reply-to": OURS,
      "auto-submitted": "auto-replied",
      subject: "Automatic reply: dinner talks",
    };

    const classification = classifyInbound(headers, "", new Set([OURS]));

    expect(classification.kind).toBe("auto_reply");
  });
});
