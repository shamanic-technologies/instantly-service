/**
 * POSTMARK-COLD-1117: the mail of our Gandi domains leaves through Postmark once
 * the domain is verified there; a Postmark refusal reads as SMTP words; Postmark
 * bounces are filed like DSNs.
 */
import { readFileSync } from "node:fs";
import { describe, it, expect } from "vitest";

import { postmarkPayload, postmarkRefusalAsSmtp } from "../../src/lib/seed-placement/postmark-transport";
import { senderAddressOf, transportForSender } from "../../src/lib/self-send/outbound";
import { filingForBounceType } from "../../src/lib/self-send/postmark-bounces";
import { recordNameInZone } from "../../src/lib/self-send/postmark-domains";
import { classifyPermanentFailure } from "../../src/lib/self-send/dispatch";

describe("which pipe a mailbox uses", () => {
  const live = new Set(["growthservice.org"]);
  it("a domain live on Postmark sends through Postmark, every other over its own SMTP", () => {
    expect(transportForSender(senderAddressOf('"Kevin Lourd" <Lourd@GrowthService.org>'), live)).toBe("postmark");
    expect(transportForSender(senderAddressOf("k.lourd@maildistribute.com"), live)).toBe("smtp");
    expect(transportForSender(senderAddressOf("kevin@growthagency.bio"), new Set())).toBe("smtp");
  });
});

describe("a Postmark refusal in SMTP words", () => {
  it("inactive / invalid recipient = a recipient bounce", () => {
    for (const code of [406, 300]) {
      const r = postmarkRefusalAsSmtp(422, code, "x");
      expect(r.kind).toBe("permanent");
      expect(classifyPermanentFailure(r.response, r.responseCode)).toBe("recipient");
    }
  });
  it("a sender-side refusal never touches the lead", () => {
    const r = postmarkRefusalAsSmtp(422, 400, "Sender signature not defined");
    expect(r.kind).toBe("permanent");
    expect(classifyPermanentFailure(r.response, r.responseCode)).toBe("sender");
  });
  it("rate limit, outage, paused sending = the mail waits", () => {
    expect(postmarkRefusalAsSmtp(429, 429, "x").kind).toBe("transient");
    expect(postmarkRefusalAsSmtp(500, 0, "x").kind).toBe("transient");
    expect(postmarkRefusalAsSmtp(422, 412, "x").kind).toBe("transient");
  });
});

describe("a follow-up through Postmark stays in the thread", () => {
  it("carries In-Reply-To, References, Cc, Bcc and attachments", () => {
    const payload = postmarkPayload(
      {
        from: "a@x.com", to: "b@y.com", cc: "c@y.com", bcc: "d@z.com", subject: "Re: s", html: "<p>h</p>", headers: {},
        inReplyTo: "<1@x.com>", references: ["<0@x.com>", "<1@x.com>"],
        attachments: [{ filename: "f.pdf", content: Buffer.from("pdf"), contentType: "application/pdf" }],
      },
      "<2@x.com>",
    );
    const headers = payload.Headers as { Name: string; Value: string }[];
    expect(headers).toContainEqual({ Name: "In-Reply-To", Value: "<1@x.com>" });
    expect(headers).toContainEqual({ Name: "References", Value: "<0@x.com> <1@x.com>" });
    expect(payload.Cc).toBe("c@y.com");
    expect(payload.Bcc).toBe("d@z.com");
    expect(payload.Attachments).toEqual([{ Name: "f.pdf", Content: Buffer.from("pdf").toString("base64"), ContentType: "application/pdf" }]);
  });
});

describe("Postmark bounces", () => {
  it("a permanent kind is a DSN the judgment sweep reads, a soft one a delay, a complaint an opt-out", () => {
    expect(filingForBounceType("HardBounce")).toBe("bounce");
    expect(filingForBounceType("Blocked")).toBe("bounce");
    expect(filingForBounceType("SoftBounce")).toBe("delay");
    expect(filingForBounceType("SpamComplaint")).toBe("unsubscribe");
    expect(filingForBounceType("AutoResponder")).toBe("ignored");
  });
});

describe("DNS", () => {
  it("writes the DKIM record under its name inside the zone", () => {
    expect(recordNameInZone("20261117pm._domainkey.growthagency.bio", "growthagency.bio")).toBe("20261117pm._domainkey");
    expect(recordNameInZone("20261117pm._domainkey", "growthagency.bio")).toBe("20261117pm._domainkey");
  });
  it("onboarding never touches MX/SPF and never takes a brand domain", () => {
    const src = readFileSync("src/lib/self-send/postmark-domains.ts", "utf8");
    expect(src).not.toMatch(/\/MX`|"MX"|"SPF"/);
    expect(src).toMatch(/NOT EXISTS \(SELECT 1 FROM instantly_domain_policy/);
  });
});

describe("every email to a person leaves through the one door", () => {
  it("sequence steps, answers and weekly seeds call sendFromMailbox, never dispatchMessage", () => {
    for (const f of ["src/lib/self-send/dispatch-worker.ts", "src/lib/reply-to-lead.ts", "src/lib/seed-placement/run.ts"]) {
      const src = readFileSync(f, "utf8");
      expect(src).toMatch(/sendFromMailbox\(/);
      expect(src).not.toMatch(/dispatchMessage\(/);
    }
  });
});
