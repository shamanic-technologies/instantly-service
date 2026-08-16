import { describe, it, expect } from "vitest";

import {
  classifyInbound,
  eventTypeForInbound,
  extractBouncedMessageIds,
  isAutoReply,
  isDeliveryStatusNotification,
  parseMessageIdList,
} from "../../src/lib/self-send/inbound";

const OURS = "<step1.camp1@saviolabsco.com>";
const KNOWN = new Set([OURS]);

describe("parseMessageIdList", () => {
  it("pulls every id out of a References chain", () => {
    expect(parseMessageIdList("<a@x.com> <b@y.com>\n <c@z.com>")).toEqual([
      "<a@x.com>",
      "<b@y.com>",
      "<c@z.com>",
    ]);
  });

  it("returns an empty list for absent or unparseable values", () => {
    expect(parseMessageIdList(undefined)).toEqual([]);
    expect(parseMessageIdList("not an id")).toEqual([]);
  });
});

describe("isAutoReply", () => {
  it.each([
    [{ "auto-submitted": "auto-replied" }],
    [{ "auto-submitted": "auto-generated" }],
    [{ "x-autoreply": "yes" }],
    [{ "x-autorespond": "vacation" }],
    [{ "x-auto-response-suppress": "OOF" }],
    [{ precedence: "auto_reply" }],
    [{ precedence: "bulk" }],
    [{ "x-auto-reply": "yes" }],
  ])("detects an autoresponder announced via %p", (headers) => {
    expect(isAutoReply(headers)).toBe(true);
  });

  // RFC 3834: `Auto-Submitted: no` is the explicit "this IS a human" marker.
  it("treats an explicit Auto-Submitted: no as human", () => {
    expect(isAutoReply({ "auto-submitted": "no" })).toBe(false);
  });

  it("treats an ordinary reply as human", () => {
    expect(isAutoReply({ from: "prospect@example.com", subject: "Re: hi" })).toBe(false);
  });
});

describe("isDeliveryStatusNotification", () => {
  it("detects the RFC 3462 report type", () => {
    expect(
      isDeliveryStatusNotification({
        "content-type": 'multipart/report; report-type=delivery-status; boundary="x"',
      }),
    ).toBe(true);
  });

  it("detects the null return-path marker", () => {
    expect(isDeliveryStatusNotification({ "return-path": "<>" })).toBe(true);
  });

  it.each([
    ["MAILER-DAEMON@googlemail.com"],
    ["Mail Delivery Subsystem <mailer-daemon@googlemail.com>"],
    ["postmaster@example.com"],
  ])("detects a daemon envelope (%s)", (from) => {
    expect(isDeliveryStatusNotification({ from })).toBe(true);
  });

  it("does not mistake an ordinary reply for a bounce", () => {
    expect(
      isDeliveryStatusNotification({
        from: "prospect@example.com",
        "content-type": "text/plain",
      }),
    ).toBe(false);
  });
});

describe("extractBouncedMessageIds", () => {
  it("recovers the id from the quoted headers in the body", () => {
    const body = [
      "Your message could not be delivered.",
      "",
      "----- Original message -----",
      `Message-ID: ${OURS}`,
      "From: amy@saviolabsco.com",
    ].join("\n");

    expect(extractBouncedMessageIds({}, body)).toContain(OURS);
  });

  it("recovers the RFC 3464 Original-Message-ID field", () => {
    expect(extractBouncedMessageIds({}, `Original-Message-ID: ${OURS}`)).toContain(OURS);
  });

  it("also reads the DSN's own In-Reply-To when it has one", () => {
    expect(extractBouncedMessageIds({ "in-reply-to": OURS }, "")).toContain(OURS);
  });

  it("de-duplicates an id found in several places", () => {
    const found = extractBouncedMessageIds(
      { "in-reply-to": OURS },
      `Message-ID: ${OURS}\nOriginal-Message-ID: ${OURS}`,
    );
    expect(found.filter((id) => id === OURS)).toHaveLength(1);
  });
});

describe("classifyInbound", () => {
  it("classifies a human reply that threads onto our send", () => {
    const result = classifyInbound(
      { "in-reply-to": OURS, from: "prospect@example.com" },
      "Sure, let's talk.",
      KNOWN,
    );
    expect(result).toEqual({ kind: "reply", referencedMessageIds: [OURS] });
  });

  // The load-bearing one. `reply_received` stops the sequence AND cancels the
  // lead's remaining holds — doing that for a vacation message would end the
  // outreach, and refund the spend, for a prospect who never engaged.
  it("classifies an out-of-office as an auto-reply, not a reply", () => {
    const result = classifyInbound(
      {
        "in-reply-to": OURS,
        "auto-submitted": "auto-replied",
        subject: "Out of office",
      },
      "I am away until Monday.",
      KNOWN,
    );
    expect(result.kind).toBe("auto_reply");
    expect(eventTypeForInbound(result.kind)).toBe("auto_reply_received");
  });

  // Some systems stamp DSNs Auto-Submitted, so bounce MUST be tested first —
  // otherwise a hard bounce files as a harmless vacation notice and the sequence
  // keeps sending to a dead address.
  it("classifies a DSN as a bounce even when it is stamped Auto-Submitted", () => {
    const result = classifyInbound(
      {
        "content-type": "multipart/report; report-type=delivery-status",
        "auto-submitted": "auto-replied",
        from: "mailer-daemon@googlemail.com",
      },
      `Message-ID: ${OURS}\n550 5.1.1 No such user`,
      KNOWN,
    );
    expect(result.kind).toBe("bounce");
    expect(eventTypeForInbound(result.kind)).toBe("email_bounced");
  });

  // These are real mailboxes that also receive ordinary mail.
  it("ignores a message that references none of our sends", () => {
    const result = classifyInbound(
      { "in-reply-to": "<someone-elses@thread.com>", from: "a@b.com" },
      "hello",
      KNOWN,
    );
    expect(result).toEqual({ kind: "unrelated", referencedMessageIds: [] });
  });

  it("ignores a cold inbound with no threading headers at all", () => {
    expect(classifyInbound({ from: "spam@x.com" }, "buy my thing", KNOWN).kind).toBe(
      "unrelated",
    );
  });

  it("ignores a bounce for a message we did not send", () => {
    const result = classifyInbound(
      { "content-type": "multipart/report; report-type=delivery-status" },
      "Message-ID: <not-ours@elsewhere.com>",
      KNOWN,
    );
    expect(result.kind).toBe("unrelated");
  });

  it("matches through the References chain, not just In-Reply-To", () => {
    const result = classifyInbound(
      { references: `<other@x.com> ${OURS}`, from: "prospect@example.com" },
      "yes",
      KNOWN,
    );
    expect(result.kind).toBe("reply");
    expect(result.referencedMessageIds).toEqual([OURS]);
  });
});

describe("eventTypeForInbound", () => {
  it("promotes nothing for an unrelated message", () => {
    expect(eventTypeForInbound("unrelated")).toBeNull();
  });
});
