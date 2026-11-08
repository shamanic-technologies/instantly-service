import { describe, expect, it } from "vitest";

import { hasVisibleText } from "../../src/lib/email-body-text";
import { ReplyToLeadBodySchema, SendRequestSchema } from "../../src/schemas";

// The four bodies that reached prospects 2026-10-28..11-03 (instantly-service
// smtp_sent_messages): each a non-empty string the old `=== ""` checks let through.
const SENT_EMPTY_IN_PROD = ["<p></p>", ",", ">", "]</think><5e9c7f0d>{"];

describe("hasVisibleText", () => {
  it.each(SENT_EMPTY_IN_PROD)("refuses %j, a body that reached a prospect empty", (body) => {
    expect(hasVisibleText(body)).toBe(false);
  });

  it.each(["", "   ", "<p>&nbsp;</p>", "<p> <br> </p>", "&mdash; &#8212; &#x2014;", "<div><p></p></div>\n"])(
    "refuses %j (tags, entities and whitespace only)",
    (body) => {
      expect(hasVisibleText(body)).toBe(false);
    },
  );

  it("refuses null and undefined", () => {
    expect(hasVisibleText(null)).toBe(false);
    expect(hasVisibleText(undefined)).toBe(false);
  });

  it.each([
    "<p>Hi Huy,</p><p>Doc Dinners runs done-for-you dinner events.</p>",
    "Hi",
    "<p>7</p>",
    "<p>Grüezi Patrick</p>",
    "<p>こんにちは</p>",
    "<p>Привет</p>",
  ])("keeps %j", (body) => {
    expect(hasVisibleText(body)).toBe(true);
  });
});

const baseSend = { to: "staff@distribute.you", subject: "Hello" };

describe("POST /orgs/send body: an empty email is refused before any spend", () => {
  it("accepts a step with readable text", () => {
    const r = SendRequestSchema.safeParse({ ...baseSend, sequence: [{ step: 1, bodyHtml: "<p>Hi there</p>", daysSinceLastStep: 0 }] });
    expect(r.success).toBe(true);
  });

  it.each(SENT_EMPTY_IN_PROD)("refuses a step whose body is %j, naming empty_email_body", (bodyHtml) => {
    const r = SendRequestSchema.safeParse({ ...baseSend, sequence: [{ step: 1, bodyHtml, daysSinceLastStep: 0 }] });
    expect(r.success).toBe(false);
    expect(JSON.stringify(r.error?.flatten())).toContain("empty_email_body");
  });

  it("refuses the whole sequence when a LATER step is empty (the Doc Dinners case: step 3 left empty by the writer)", () => {
    const r = SendRequestSchema.safeParse({
      ...baseSend,
      sequence: [
        { step: 1, bodyHtml: "<p>Christine, a quick note.</p>", daysSinceLastStep: 0 },
        { step: 2, bodyHtml: "<p></p>", daysSinceLastStep: 3 },
      ],
    });
    expect(r.success).toBe(false);
  });
});

describe("POST /orgs/replies body", () => {
  it("refuses a reply with no readable text", () => {
    const r = ReplyToLeadBodySchema.safeParse({ email: "staff@distribute.you", body_html: "<p>,</p>" });
    expect(r.success).toBe(false);
    expect(r.error?.message).toContain("empty_email_body");
  });

  it("accepts a reply with text", () => {
    const r = ReplyToLeadBodySchema.safeParse({ email: "staff@distribute.you", body_html: "<p>Thanks, Tuesday works.</p>" });
    expect(r.success).toBe(true);
  });
});
