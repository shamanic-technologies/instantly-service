/**
 * SES-COLD-1119: the mail of our Gandi domains leaves through Amazon SES once
 * the domain is verified there; SES's rewritten Message-Id is the one stored;
 * a throttle waits; SES bounces/complaints arrive by signed SNS and are filed
 * like DSNs; a kill switch puts every domain back on SMTP.
 */
import { generateKeyPairSync, createSign } from "node:crypto";
import { readFileSync } from "node:fs";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import { signRequest, sesSmtpPassword } from "../../src/lib/aws/sigv4";
import { assertNotTransactional, coldTopicArn, xmlValue, type SesAccount } from "../../src/lib/aws/ses-account";
import { pipeForSender, senderAddressOf, SES_KILL_SWITCH_ENV, sesColdSendingDisabled } from "../../src/lib/self-send/outbound";
import {
  resetSesSlots,
  sendThroughSes,
  sesIdFromResponse,
  sesIdFromRfcMessageId,
  sesMaxPerSecond,
  waitForSesSlot,
  withMessageId,
} from "../../src/lib/self-send/ses-transport";
import { sesDnsRecords, sesLiveVerdict, identityStateOf, SES_COLD_EVENT_TYPES } from "../../src/lib/self-send/ses-domains";
import {
  filingForSesEvent,
  isSnsUrl,
  sesEventWords,
  snsStringToSign,
  verifySnsSignature,
  type SnsEnvelope,
} from "../../src/lib/self-send/ses-events";
import { buildMessage } from "../../src/lib/self-send/message";
import { classifyDispatchFailure, SmtpDispatchError, type DispatchResult } from "../../src/lib/self-send/smtp";
import { classifyPermanentFailure } from "../../src/lib/self-send/dispatch";
import type { Account } from "../../src/lib/instantly-client";

const account: SesAccount = {
  accessKeyId: "AKIAEXAMPLE",
  secretAccessKey: "secret",
  region: "us-east-1",
  account: "168197326881",
  smtpUser: "AKIAEXAMPLE",
  smtpPassword: "pw",
  smtpHost: "email-smtp.us-east-1.amazonaws.com",
  topicArn: coldTopicArn("us-east-1", "168197326881"),
};

describe("AWS SigV4 by hand", () => {
  it("matches AWS's published get-vanilla test vector", () => {
    const headers = signRequest({
      method: "GET",
      url: "https://example.amazonaws.com/",
      service: "service",
      credentials: { accessKeyId: "AKIDEXAMPLE", secretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", region: "us-east-1" },
      now: new Date("2015-08-30T12:36:00Z"),
    });
    expect(headers.Authorization).toBe(
      "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
    );
  });
  it("derives the SES SMTP password (version byte 4 + 32-byte HMAC)", () => {
    const raw = Buffer.from(sesSmtpPassword("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "us-east-1"), "base64");
    expect(raw.length).toBe(33);
    expect(raw[0]).toBe(4);
  });
  it("reads SNS XML answers", () => {
    expect(xmlValue("<CreateTopicResult><TopicArn>arn:x</TopicArn></CreateTopicResult>", "TopicArn")).toBe("arn:x");
  });
});

describe("never the transactional SES account", () => {
  it("refuses a marketing key that is the transactional key or account", () => {
    expect(() => assertNotTransactional({ accessKeyId: "A", account: "1" }, { accessKeyId: "A", account: "2" })).toThrow(/TRANSACTIONAL/);
    expect(() => assertNotTransactional({ accessKeyId: "A", account: "1" }, { accessKeyId: "B", account: "1" })).toThrow(/TRANSACTIONAL/);
    expect(() => assertNotTransactional({ accessKeyId: "A", account: "1" }, { accessKeyId: "B", account: "2" })).not.toThrow();
    expect(() => assertNotTransactional({ accessKeyId: "A", account: "1" }, null)).not.toThrow();
  });
  it("the cold code never names the transactional key as a sender", () => {
    for (const f of ["src/lib/self-send/ses-transport.ts", "src/lib/self-send/ses-domains.ts"]) {
      expect(readFileSync(f, "utf8")).not.toMatch(/aws-ses-transactional/);
    }
  });
});

describe("one pipe per domain, decided in one place", () => {
  const live = { ses: new Set(["growthservice.org", "molthost.org"]), postmark: new Set(["growthservice.org"]) };
  it("SES beats Postmark, Postmark beats SMTP, everything else is SMTP", () => {
    expect(pipeForSender(senderAddressOf('"Kevin Lourd" <K.Lourd@GrowthService.org>'), live, false)).toBe("ses");
    expect(pipeForSender("bria@molthost.org", live, false)).toBe("ses");
    expect(pipeForSender("x@other.org", { ses: new Set(), postmark: new Set(["other.org"]) }, false)).toBe("postmark");
    expect(pipeForSender("kevin@salesmolt.com", live, false)).toBe("smtp");
  });
  it("the kill switch puts every SES domain back on its mailbox's SMTP", () => {
    expect(pipeForSender("bria@molthost.org", live, true)).toBe("smtp");
    expect(pipeForSender("k@growthservice.org", live, true)).toBe("smtp");
  });
  describe("kill switch env, read at each send", () => {
    afterEach(() => {
      delete process.env[SES_KILL_SWITCH_ENV];
    });
    it("only the exact word true disables", () => {
      expect(SES_KILL_SWITCH_ENV).toBe("SES_COLD_SENDING_DISABLED");
      expect(sesColdSendingDisabled()).toBe(false);
      process.env[SES_KILL_SWITCH_ENV] = "TRUE";
      expect(sesColdSendingDisabled()).toBe(true);
      process.env[SES_KILL_SWITCH_ENV] = "false";
      expect(sesColdSendingDisabled()).toBe(false);
    });
  });
});

const built = buildMessage({
  account: { email: "k.lourd@growthservice.org", first_name: "Kevin", last_name: "Lourd" } as Account,
  signatureHtml: null,
  leadEmail: "seed@receiver.com",
  subject: "Quick question",
  bodyHtml: "<p>Hi</p>",
  step: 1,
  identity: { instantlyCampaignId: "self:x", leadEmail: "seed@receiver.com" },
  optOut: "mailbox_only",
});

describe("SES rewrites the Message-Id: we store the one recipients see", () => {
  it("reads the SES id out of `250 Ok <id>`", () => {
    expect(sesIdFromResponse("250 Ok 0100019a8b7c6d5e-1234abcd-5678-90ef-aaaa-bbbbccccdddd-000000")).toBe(
      "0100019a8b7c6d5e-1234abcd-5678-90ef-aaaa-bbbbccccdddd-000000",
    );
    expect(sesIdFromResponse("250 2.0.0 OK")).toBeNull();
    expect(sesIdFromRfcMessageId("<0100019a-x-000000@email.amazonses.com>")).toBe("0100019a-x-000000");
  });

  it("returns <id@email.amazonses.com>, sends through our configuration set, rewrites the recorded bytes", async () => {
    resetSesSlots();
    const dispatch = vi.fn(async (): Promise<DispatchResult> => ({
      messageId: "<ours@growthservice.org>",
      response: "250 Ok 0100019a-sesid-000000",
      accepted: ["seed@receiver.com"],
      rejected: [],
      rawMessage: Buffer.from("From: a\r\nMessage-ID: <ours@growthservice.org>\r\nTo: b\r\n\r\nbody Message-ID: <ours@growthservice.org>"),
    }));
    const sent = await sendThroughSes("k.lourd@growthservice.org", built, { account, dispatch });
    expect(sent.messageId).toBe("<0100019a-sesid-000000@email.amazonses.com>");
    const [credential, message] = dispatch.mock.calls[0] as unknown as [{ smtpHost: string; authUser: string; address: string }, { headers: Record<string, string> }];
    expect(credential.smtpHost).toBe("email-smtp.us-east-1.amazonaws.com");
    expect(credential.authUser).toBe("AKIAEXAMPLE");
    expect(message.headers["X-SES-CONFIGURATION-SET"]).toBe("instantly-cold-email");
    const raw = sent.rawMessage.toString("latin1");
    expect(raw).toContain("Message-ID: <0100019a-sesid-000000@email.amazonses.com>\r\nTo: b");
    expect(raw).toContain("body Message-ID: <ours@growthservice.org>");
  });

  it("replaces a folded Message-ID header too", () => {
    const out = withMessageId(Buffer.from("A: 1\r\nMessage-ID:\r\n <x@y>\r\nB: 2\r\n\r\nbody"), "<s@email.amazonses.com>").toString();
    expect(out).toBe("A: 1\r\nMessage-ID: <s@email.amazonses.com>\r\nB: 2\r\n\r\nbody");
  });

  it("the next step threads on the SES id: In-Reply-To and References name the real first email", () => {
    const first = "<0100019a-first-000000@email.amazonses.com>";
    const followUp = buildMessage({
      account: { email: "k.lourd@growthservice.org", first_name: "Kevin", last_name: "Lourd" } as Account,
      signatureHtml: null,
      leadEmail: "lead@x.com",
      subject: "Re: Quick question",
      bodyHtml: "<p>Following up</p>",
      step: 2,
      identity: { instantlyCampaignId: "self:x", leadEmail: "lead@x.com" },
      previousMessageId: first,
      priorMessageIds: [first],
      optOut: "mailbox_only",
    });
    expect(followUp.inReplyTo).toBe(first);
    expect(followUp.references).toEqual([first]);
  });
});

describe("rate and refusals", () => {
  beforeEach(() => resetSesSlots());
  afterEach(() => {
    delete process.env.SES_COLD_MAX_PER_SECOND;
  });
  it("never more than the per-second budget (default 5, capped at SES's 14)", async () => {
    expect(sesMaxPerSecond()).toBe(5);
    process.env.SES_COLD_MAX_PER_SECOND = "50";
    expect(sesMaxPerSecond()).toBe(14);
    process.env.SES_COLD_MAX_PER_SECOND = "4";
    const waits: number[] = [];
    for (let i = 0; i < 4; i += 1) await waitForSesSlot(() => 1000, async (ms) => void waits.push(ms));
    expect(waits).toEqual([250, 500, 750]);
  });
  it("a throttle is transient (the mail waits), never a bounce", () => {
    expect(classifyDispatchFailure({ responseCode: 454, response: "454 Throttling failure: Maximum sending rate exceeded." })).toBe("transient");
    expect(classifyDispatchFailure({ response: "454 Throttling failure: Daily message quota exceeded." })).toBe("transient");
  });
  it("an SES refusal of OUR identity / account is a sender fault, the lead untouched", () => {
    expect(classifyPermanentFailure("554 Message rejected: Email address is not verified.", 554)).toBe("sender");
    expect(classifyPermanentFailure("554 Account sending paused", 554)).toBe("sender");
  });
  it("a transient SES refusal surfaces as SmtpDispatchError transient", async () => {
    const dispatch = vi.fn(async () => {
      throw new SmtpDispatchError("transient", 454, "454 Throttling failure", "throttled");
    });
    await expect(sendThroughSes("k@growthservice.org", built, { account, dispatch })).rejects.toMatchObject({ kind: "transient" });
  });
});

describe("onboarding: DNS and verdict", () => {
  it("writes only the 3 DKIM CNAMEs and the MAIL FROM subdomain's MX + SPF, never the apex", () => {
    const records = sesDnsRecords("us-east-1", ["t1", "t2", "t3"]);
    expect(records).toEqual([
      { name: "t1._domainkey", type: "CNAME", value: "t1.dkim.amazonses.com." },
      { name: "t2._domainkey", type: "CNAME", value: "t2.dkim.amazonses.com." },
      { name: "t3._domainkey", type: "CNAME", value: "t3.dkim.amazonses.com." },
      { name: "ses", type: "MX", value: "10 feedback-smtp.us-east-1.amazonses.com." },
      { name: "ses", type: "TXT", value: '"v=spf1 include:amazonses.com ~all"' },
    ]);
    const src = readFileSync("src/lib/self-send/ses-domains.ts", "utf8");
    expect(src).toMatch(/record\.type === "MX" && record\.name !== SES_MAIL_FROM_LABEL/);
    expect(src).toMatch(/loadOnboardingCandidates/);
  });
  it("live only when identity, DKIM, MAIL FROM, our configuration set AND the event subscription all hold", () => {
    const ok = identityStateOf(200, {
      VerifiedForSendingStatus: true,
      DkimAttributes: { Status: "SUCCESS", Tokens: ["a", "b", "c"] },
      MailFromAttributes: { MailFromDomain: "ses.molthost.org", MailFromDomainStatus: "SUCCESS" },
      ConfigurationSetName: "instantly-cold-email",
    });
    expect(sesLiveVerdict("molthost.org", ok, true)).toEqual({ live: true, reason: null });
    expect(sesLiveVerdict("molthost.org", ok, false).reason).toBe("events_not_confirmed");
    expect(sesLiveVerdict("molthost.org", { ...ok, dkimStatus: "PENDING" }, true).reason).toBe("dkim_pending");
    expect(sesLiveVerdict("molthost.org", { ...ok, mailFromStatus: "PENDING" }, true).reason).toBe("mail_from_pending");
    expect(sesLiveVerdict("molthost.org", { ...ok, configurationSet: "distribute-newsletter-events" }, true).reason).toBe("configuration_set_not_ours");
    expect(sesLiveVerdict("molthost.org", identityStateOf(404, {}), true).reason).toBe("identity_missing");
  });
  it("our events: bounces, complaints, delays, rejects", () => {
    expect([...SES_COLD_EVENT_TYPES].sort()).toEqual(["BOUNCE", "COMPLAINT", "DELIVERY_DELAY", "REJECT"]);
  });
});

describe("SES events by SNS", () => {
  const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const pem = publicKey.export({ type: "spki", format: "pem" }).toString();
  function signed(m: Omit<SnsEnvelope, "Signature">): SnsEnvelope {
    const signer = createSign("RSA-SHA256");
    signer.update(snsStringToSign(m as SnsEnvelope));
    return { ...m, Signature: signer.sign(privateKey, "base64") } as SnsEnvelope;
  }
  const base = {
    Type: "Notification",
    MessageId: "sns-1",
    TopicArn: account.topicArn,
    Message: JSON.stringify({ eventType: "Bounce" }),
    Timestamp: "2026-11-19T10:00:00.000Z",
    SignatureVersion: "2",
    SigningCertURL: "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-abc.pem",
  };

  it("verifies a real signature and refuses a tampered message or a foreign certificate host", async () => {
    const m = signed(base);
    expect(await verifySnsSignature(m, async () => pem)).toBe(true);
    expect(await verifySnsSignature({ ...m, Message: "{}" }, async () => pem)).toBe(false);
    expect(await verifySnsSignature({ ...m, SigningCertURL: "https://evil.example.com/x.pem" }, async () => pem)).toBe(false);
    expect(isSnsUrl("http://sns.us-east-1.amazonaws.com/x.pem", "cert")).toBe(false);
    expect(isSnsUrl("https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription", "subscribe")).toBe(true);
  });

  it("signs a subscription confirmation over its own keys", () => {
    const s = snsStringToSign({ ...base, Type: "SubscriptionConfirmation", SubscribeURL: "u", Token: "t", Signature: "" });
    expect(s).toBe(`Message\n${base.Message}\nMessageId\nsns-1\nSubscribeURL\nu\nTimestamp\n${base.Timestamp}\nToken\nt\nTopicArn\n${base.TopicArn}\nType\nSubscriptionConfirmation\n`);
  });

  it("a permanent bounce is a DSN for the judgment sweep, a soft one a delay, a complaint an opt-out", () => {
    expect(filingForSesEvent({ eventType: "Bounce", bounce: { bounceType: "Permanent", bounceSubType: "General" } })).toBe("bounce");
    expect(filingForSesEvent({ eventType: "Bounce", bounce: { bounceType: "Transient" } })).toBe("delay");
    expect(filingForSesEvent({ eventType: "Bounce", bounce: { bounceType: "Undetermined" } })).toBe("delay");
    expect(filingForSesEvent({ eventType: "DeliveryDelay" })).toBe("delay");
    expect(filingForSesEvent({ eventType: "Complaint" })).toBe("unsubscribe");
    expect(filingForSesEvent({ notificationType: "Complaint" })).toBe("unsubscribe");
    expect(filingForSesEvent({ eventType: "Reject" })).toBe("ignored");
  });

  it("an address on the account suppression list is a RECIPIENT-side bounce with the report's words", () => {
    const event = {
      eventType: "Bounce",
      bounce: {
        bounceType: "Permanent",
        bounceSubType: "OnAccountSuppressionList",
        bouncedRecipients: [{ emailAddress: "gone@x.com", action: "failed", status: "5.1.1", diagnosticCode: "Amazon SES did not send the message to this address because it is on the suppression list for your account." }],
      },
    };
    expect(filingForSesEvent(event)).toBe("bounce");
    const words = sesEventWords(event);
    expect(words).toMatch(/suppression list/);
    expect(words).toMatch(/not a problem with the sender/);
    expect(words).toMatch(/Status: 5\.1\.1/);
  });
});

describe("every email to a person still leaves through the one door", () => {
  it("only outbound.ts reaches SES", () => {
    for (const f of ["src/lib/self-send/dispatch-worker.ts", "src/lib/reply-to-lead.ts", "src/lib/seed-placement/run.ts"]) {
      const src = readFileSync(f, "utf8");
      expect(src).toMatch(/sendFromMailbox\(/);
      expect(src).not.toMatch(/sendThroughSes\(/);
    }
  });
});
