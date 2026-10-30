/**
 * UNIBOX-SIGNED-BODY-1030: a dispatched email is served as the prospect got it.
 * One case per send path: a recorded sequence step (exact html), a step sent
 * before the record (re-rendered with the dispatch builder: ours, and a
 * client's signature with sender placeholders), a manual reply (stored signed),
 * and the cases whose sent version cannot be known (served as stored, flagged).
 */
import { describe, expect, it, vi } from "vitest";

import { buildMessage } from "../../src/lib/self-send/message";
import { htmlToText } from "../../src/lib/forward-positive-reply";
import {
  logUnsignedSteps,
  sentBodyHtml,
  SIGNATURE_ONLY_SINCE,
} from "../../src/lib/self-send/sent-body";
import type { Account } from "../../src/lib/instantly-client";

const STEP_BODY = "<p>Hi Stefan,</p><p>Worth a look: ascendqe.com</p>";
const ELIZABETH = { email: "elizabeth@capebinaryio.com", first_name: "Elizabeth", last_name: "Thomas" } as Account;

/** What the dispatch worker hands to SMTP for this step (no link tracking, no env). */
function dispatched(account: Account, signatureHtml: string | null): string {
  return buildMessage({
    account,
    signatureHtml,
    leadEmail: "stefan.schweigert@vertigis.com",
    subject: "Quick question",
    bodyHtml: STEP_BODY,
    step: 3,
    identity: { instantlyCampaignId: "self:e88fd08a", leadEmail: "stefan.schweigert@vertigis.com" },
    unsubscribeUrl: "https://u.test/u/x/y",
    trackingOrigin: null,
  }).html;
}

const base = {
  bodyHtml: STEP_BODY,
  sequenceStep: true,
  dispatchedAt: "2026-10-20T14:45:49Z",
  accountKnown: true,
  accountEmail: ELIZABETH.email,
  accountFirstName: "Elizabeth",
  accountLastName: "Thomas",
};

describe("sentBodyHtml", () => {
  it("serves the recorded html we handed to SMTP, byte-equal", () => {
    const html = dispatched(ELIZABETH, null);
    const served = sentBodyHtml({ ...base, sentHtml: html, dispatchedAt: "2026-10-29T14:45:49Z" });
    expect(served).toEqual({ bodyHtml: html, source: "recorded" });
    expect(htmlToText(served.bodyHtml)).toMatch(/ascendqe\.com\n\n--\n\nElizabeth Thomas\nDistribute\.you \| Marketing Agency$/);
  });

  it("re-renders an unrecorded step with OUR signature, equal to what dispatch built", () => {
    const served = sentBodyHtml(base);
    expect(served.source).toBe("rendered");
    expect(served.bodyHtml).toBe(dispatched(ELIZABETH, null));
  });

  it("re-renders a client's pinned signature with the sender placeholders filled, byte-equal", () => {
    const pin = "<p>{full_name}<br>Head of Growth, Acme<br>{sender_email}</p>";
    const served = sentBodyHtml({ ...base, pinnedSignatureHtml: pin });
    expect(served.bodyHtml).toBe(dispatched(ELIZABETH, pin));
    expect(htmlToText(served.bodyHtml)).toContain("Elizabeth Thomas\nHead of Growth, Acme\nelizabeth@capebinaryio.com");
    expect(served.bodyHtml).not.toContain("{");
  });

  it("serves a manual reply as stored (it is stored signed)", () => {
    const signed = "<p>Thursday works.</p><p>--</p><p>Elizabeth Thomas<br>Distribute.you | Marketing Agency</p>";
    expect(sentBodyHtml({ ...base, sequenceStep: false, bodyHtml: signed })).toEqual({ bodyHtml: signed, source: "stored" });
  });

  it("never guesses: a step sent while a footer line still followed the signature is served as stored", () => {
    const served = sentBodyHtml({ ...base, dispatchedAt: new Date(SIGNATURE_ONLY_SINCE.getTime() - 1) });
    expect(served).toEqual({ bodyHtml: STEP_BODY, source: "stored", unsignedReason: "sent_before_signature_only" });
  });

  it("never guesses: an unknown sending account is served as stored", () => {
    expect(sentBodyHtml({ ...base, accountKnown: false })).toEqual({
      bodyHtml: STEP_BODY,
      source: "stored",
      unsignedReason: "account_unknown",
    });
  });

  it("logs loud only when a step is served unsigned", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    logUnsignedSteps("self:x", [{ dispatchId: "d1", body: sentBodyHtml(base) }]);
    expect(warn).not.toHaveBeenCalled();
    logUnsignedSteps("self:x", [{ dispatchId: "d2", body: sentBodyHtml({ ...base, accountKnown: false }) }]);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("d2=account_unknown"));
    warn.mockRestore();
  });
});
