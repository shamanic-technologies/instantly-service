/**
 * SIG-NO-DASHES-1117 (owner 2026-11-17): the reader never sees a `--` line
 * between an email's body and its signature, on either transport. The boundary
 * is an invisible marker (a zero-box wrapper); bodies built before the change
 * (`<p>--</p>`) are still read and stripped exactly as before.
 */
import { describe, expect, it } from "vitest";

import {
  buildEmailBodyWithSignature,
  buildReplyBodyWithSignature,
  buildSequenceSteps,
  SIGNATURE_WRAPPER_OPEN,
  signatureBlockHtml,
  stripAccountSignature,
} from "../../src/lib/send-lead";
import { buildMessage } from "../../src/lib/self-send/message";
import { htmlToText } from "../../src/lib/forward-positive-reply";
import { cleanForSpeech } from "../../src/lib/positive-reply-item";
import type { Account } from "../../src/lib/instantly-client";

const SOPHIE = { email: "sophie@hidistribute.com", first_name: "Sophie", last_name: "Martin" } as Account;
const PIN = "<p>Sophie<br>AI introductions, distribute.you</p>";
const BODY = "<p>Hi Jane,</p><p>Happy to introduce you to Tom.</p><p>Cheers,</p>";

/** What a mail client shows, as text lines: every tag gone, nothing converted. */
function renderedLines(html: string): string[] {
  return html
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div)>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/gi, " ")
    .split("\n")
    .map((l) => l.trim());
}

function expectNoVisibleSeparator(html: string): void {
  expect(renderedLines(html).filter((l) => /^-{2,}$/.test(l))).toEqual([]);
  expect(html).not.toMatch(/>\s*--/);
}

describe("SIG-NO-DASHES-1117: no visible `--` between body and signature", () => {
  it("Instantly transport: every sequence step ends on the signature with no `--` line", () => {
    const steps = buildSequenceSteps("Intro", [
      { step: 1, bodyHtml: BODY, daysSinceLastStep: 0 },
      { step: 2, bodyHtml: "<p>Following up.</p><p>Cheers,</p>", daysSinceLastStep: 3 },
    ], SOPHIE, PIN);
    for (const step of steps) {
      expectNoVisibleSeparator(step.bodyHtml);
      expect(step.bodyHtml.endsWith(signatureBlockHtml(PIN))).toBe(true);
    }
  });

  it("self-send transport: the message handed to SMTP carries no `--` line", () => {
    const msg = buildMessage({
      account: SOPHIE,
      signatureHtml: PIN,
      leadEmail: "kevin+sig@hidistribute.com",
      subject: "Intro",
      bodyHtml: BODY,
      step: 1,
      identity: { instantlyCampaignId: "self:sig", leadEmail: "kevin+sig@hidistribute.com" },
      unsubscribeUrl: "https://u.test/u/x/y",
      optOut: "tracker_link",
    });
    expectNoVisibleSeparator(msg.html);
    expect(msg.html).toContain(`<p>Cheers,</p>${SIGNATURE_WRAPPER_OPEN}${PIN}</div>`);
  });

  it("a reply and our default signature carry no `--` line either", () => {
    expectNoVisibleSeparator(buildReplyBodyWithSignature("<p>Thursday works.</p>", SOPHIE));
    expectNoVisibleSeparator(buildEmailBodyWithSignature(BODY, SOPHIE));
  });

  it("the marker renders nothing of its own: no text, no hidden style", () => {
    expect(SIGNATURE_WRAPPER_OPEN).toBe('<div style="margin:0;padding:0;border:0">');
    expect(SIGNATURE_WRAPPER_OPEN).not.toMatch(/display|font-size|visibility|color/);
  });

  it("idempotent on the new marker: f(f(x)) === f(x), three stacked collapse to one", () => {
    const once = buildEmailBodyWithSignature(BODY, SOPHIE, PIN);
    expect(buildEmailBodyWithSignature(once, SOPHIE, PIN)).toBe(once);
    const stacked = BODY + signatureBlockHtml("<p>A</p>") + signatureBlockHtml("<p>B</p>") + signatureBlockHtml("<p>C</p>");
    expect(buildEmailBodyWithSignature(stacked, SOPHIE, PIN)).toBe(once);
  });

  it("strips the marker as a sanitizer may respace it", () => {
    expect(stripAccountSignature(`${BODY}<div style="margin: 0px; padding: 0px; border: 0px;"><p>Old</p></div>`)).toBe(BODY);
    expect(stripAccountSignature(`${BODY}<DIV STYLE="margin:0; padding:0; border:0"><p>Old</p></DIV>`)).toBe(BODY);
  });

  it("a writer's own div is never read as the marker", () => {
    const body = '<div style="margin:0">Hi</div><div style="padding:0;border:0">There</div>';
    expect(stripAccountSignature(body)).toBe(body);
  });

  it("regression: a body stored with the old `<p>--</p>` separator is stripped exactly as before", () => {
    const stored = `${BODY}<p>--</p><p>Kevin Lourd<br>Distribute.you | Marketing Agency</p>`;
    expect(stripAccountSignature(stored)).toBe(BODY);
    const rebuilt = buildEmailBodyWithSignature(stored, SOPHIE, PIN);
    expect(rebuilt).toBe(buildEmailBodyWithSignature(BODY, SOPHIE, PIN));
    expectNoVisibleSeparator(rebuilt);
  });

  it("text readers still find the signature: the spoken thread cuts it off", () => {
    const built = buildEmailBodyWithSignature(BODY, SOPHIE, PIN);
    const text = htmlToText(built);
    expect(text).toContain("Cheers,\n\n--\n\nSophie");
    expect(cleanForSpeech(text)).toBe("Hi Jane,\n\nHappy to introduce you to Tom.\n\nCheers,");
  });
});
