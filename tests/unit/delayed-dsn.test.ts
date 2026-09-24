import { describe, it, expect } from "vitest";

import {
  classifyInbound,
  eventTypeForInbound,
  isTransientDeliveryReport,
} from "../../src/lib/self-send/inbound";
import {
  planDelayedDsnBackfill,
  type BouncedEvent,
  type DsnRow,
} from "../../src/lib/delayed-dsn-backfill";

// ─── Real DSN text, captured from prod `imap_messages_raw.payload->>'textSnippet'`
// (2026-09-24). Addresses kept as captured; only the Message-Id is pinned to ours.

const OURS = "<019ff5de-4e36-77c2-b2f6-d4ec1ca2254d@maildistribute.com>";
const KNOWN = new Set([OURS]);

const GMAIL_DELAY = `** Delivery incomplete **

There was a temporary problem delivering your message to stephanie.jackson@amenitynursingcare.com. Gmail will retry for 46 more hours. You'll be notified if the delivery fails permanently.



The response from the remote server was:
451 4.4.4 Mail received as unauthenticated, incoming to a recipient domain configured in a hosted tenant which has no mail-enabled subscriptions. ATTR5 [BN1PEPF0000468D.namprd05.prod.outlook.com 2026-08-13T13:12:35.600Z 08DEF8E545D8505D]

Reporting-MTA: dns; googlemail.com
Received-From-MTA: dns; k.lourd@maildistribute.com
Arrival-Date: Wed, 12 Aug 2026 05:06:55 -0700 (PDT)
X-Original-Message-ID: ${OURS}

Final-Recipient: rfc822; stephanie.jackson@amenitynursingcare.com
Action: delayed
Status: 4.4.4
Remote-MTA: dns; amenitynursingcare-com.mail.protection.outlook.com.
 (2a01:111:f403:f908::2, the server for the domain amenitynursingcare.com.)
Diagnostic-Code: smtp; 451 4.4.4 Mail received as unauthenticated, incoming to a recipient domain configured in a hosted tenant which has no mail-enabled subscriptions. ATTR5 [BN1PEPF0000468D.namprd05.prod.outlook.com 2026-08-13T13:12:35.600Z 08DEF8E545D8505D]
Last-Attempt-Date: Thu, 13 Aug 2026 06:12:35 -0700 (PDT)
Will-Retry-Until: Sat, 15 Aug 2026 05:06:55 -0700 (PDT)
`;

// A Gmail delay notice whose stored snippet carries no machine-readable part.
const GMAIL_DELAY_NO_REPORT = `** Delivery incomplete **

There was a temporary problem delivering your message to molly@4mhealthlabs.com. Gmail will retry for 22 more hours. You'll be notified if the delivery fails permanently.

Learn more here: https://support.google.com/mail/answer/7720

The response was:

The outbound connection was established to the server for the recipient domain 4mhealthlabs.com by 4mhealthlabs-com.mail.protection.outlook.com. [2a01:111:f403:f90d::1], but was unable to complete the message transaction, either because of time-out, or inadequate connection quality. For more information, go to https://support.google.com/mail/answer/7720
Message-ID: ${OURS}
`;

const ADDRESS_NOT_FOUND = `** Address not found **

Your message wasn't delivered to shanelle@bigskylaw.com because the address couldn't be found, or is unable to receive mail.

Learn more here: https://aka.ms/EXOSmtpErrors
(Warning: This link will take you to a third-party site)

The response from the remote server was:
550 5.4.1 Recipient address rejected: Access denied. For more information see https://aka.ms/EXOSmtpErrors [BL02EPF0002992B.namprd02.prod.outlook.com 2026-08-25T03:11:20.050Z 08DEFF3F4EB442D8]

Reporting-MTA: dns; googlemail.com
Received-From-MTA: dns; k.lourd@maildistribute.com
Arrival-Date: Mon, 24 Aug 2026 20:11:18 -0700 (PDT)
X-Original-Message-ID: ${OURS}

Final-Recipient: rfc822; shanelle@bigskylaw.com
Action: failed
Status: 5.4.1
Remote-MTA: dns; bigskylaw-com.mail.protection.outlook.com.
 (2a01:111:f403:c922::2, the server for the domain bigskylaw.com.)
Diagnostic-Code: smtp; 550 5.4.1 Recipient address rejected: Access denied. For more information see https://aka.ms/EXOSmtpErrors [BL02EPF0002992B.namprd02.prod.outlook.com 2026-08-25T03:11:20.050Z 08DEFF3F4EB442D8]
Last-Attempt-Date: Mon, 24 Aug 2026 20:11:20 -0700 (PDT)
`;

const MESSAGE_BLOCKED = `** Message blocked **

Your message to dpowell@mms.med.pro has been blocked. See technical details below for more information.

Learn more here: https://community.mimecast.com/docs/DOC-1369#554
(Warning: This link will take you to a third-party site)

The response from the remote server was:
554 Email rejected due to security policies - https://community.mimecast.com/docs/DOC-1369#554 [OHw3LfR8PeebakDjLtUkDQ.usb72]

Reporting-MTA: dns; googlemail.com
Received-From-MTA: dns; k.lourd@maildistribute.com
Arrival-Date: Wed, 02 Sep 2026 07:29:23 -0700 (PDT)
X-Original-Message-ID: ${OURS}

Final-Recipient: rfc822; dpowell@mms.med.pro
Action: failed
Status: 5.7.0
Remote-MTA: dns; usb-smtp-inbound-1.mimecast.com. (170.10.152.242, the server
 for the domain mms.med.pro.)
Diagnostic-Code: smtp; 554 Email rejected due to security policies - https://community.mimecast.com/docs/DOC-1369#554 [OHw3LfR8PeebakDjLtUkDQ.usb72]
Last-Attempt-Date: Wed, 02 Sep 2026 07:29:29 -0700 (PDT)
`;

// Outlook NDR: no RFC 3464 fields, and it quotes a 450 4.x line BESIDE the 550
// 5.x one — which is why free-text SMTP codes are deliberately never read.
const OUTLOOK_NDR = `Delivery has failed to these recipients or groups:

dfutch@ghcscw.com<mailto:dfutch@ghcscw.com>
Your message wasn't delivered. Despite repeated attempts to deliver your message, a connection to the remote server couldn't be made.

Diagnostic information for administrators:

Generating server: SJ2PR16MB5890.namprd16.prod.outlook.com
Receiving server: SJ2PR16MB5890.namprd16.prod.outlook.com

dfutch@ghcscw.com
8/26/2026 3:27:58 AM - Server at SJ2PR16MB5890.namprd16.prod.outlook.com returned '550 5.4.317 Message expired, cannot connect to remote server(Failed to connect. Winsock error code: 995, Win32 error code: 995)'
8/26/2026 3:27:28 AM - Server at webmail.ghcscw.com (75.141.38.249) returned '450 4.4.317 Cannot establish session with remote server'
Message-ID: ${OURS}
`;

const GMAIL_HEADERS = (subject: string) => ({
  from: '"Mail Delivery Subsystem" <mailer-daemon@googlemail.com>',
  subject,
  "content-type": "multipart/report; report-type=delivery-status",
  "auto-submitted": "auto-replied",
});
const DELAY_HEADERS = GMAIL_HEADERS("Delivery Status Notification (Delay)");
const FAILURE_HEADERS = GMAIL_HEADERS("Delivery Status Notification (Failure)");
const OUTLOOK_HEADERS = {
  from: "postmaster@outlook.com",
  subject: "Undeliverable: How does chiropractic patient growth work at GHC?",
  "content-type": "multipart/report; report-type=delivery-status",
};

describe("a temporary delivery delay is not a bounce", () => {
  it("Gmail 'Delivery incomplete' (Action: delayed, 4.4.4) → delay, promotes nothing", () => {
    const c = classifyInbound(DELAY_HEADERS, GMAIL_DELAY, KNOWN);
    expect(c.kind).toBe("delay");
    expect(c.referencedMessageIds).toEqual([OURS]);
    expect(eventTypeForInbound(c.kind)).toBeNull();
  });

  it("Gmail delay notice with no report part → still a delay (Gmail's own marker)", () => {
    expect(isTransientDeliveryReport(DELAY_HEADERS, GMAIL_DELAY_NO_REPORT)).toBe(true);
    expect(classifyInbound(DELAY_HEADERS, GMAIL_DELAY_NO_REPORT, KNOWN).kind).toBe("delay");
  });

  it("a 4.x.x Status with no Action field → delay", () => {
    expect(isTransientDeliveryReport({}, "Final-Recipient: rfc822; a@b.com\nStatus: 4.2.2\n")).toBe(true);
  });
});

describe("a permanent failure still is", () => {
  it.each([
    ["Address not found (Action: failed, 5.4.1)", FAILURE_HEADERS, ADDRESS_NOT_FOUND],
    ["Message blocked (Action: failed, 5.7.0)", FAILURE_HEADERS, MESSAGE_BLOCKED],
    ["Outlook NDR quoting 450 4.x beside 550 5.x", OUTLOOK_HEADERS, OUTLOOK_NDR],
  ])("%s → email_bounced", (_label, headers, body) => {
    const c = classifyInbound(headers, body, KNOWN);
    expect(c.kind).toBe("bounce");
    expect(eventTypeForInbound(c.kind)).toBe("email_bounced");
  });

  it("a report where one recipient failed and another is delayed is a bounce", () => {
    const body = "Action: delayed\nStatus: 4.4.1\n\nAction: failed\nStatus: 5.1.1\n";
    expect(isTransientDeliveryReport({}, body)).toBe(false);
  });

  it("any 5.x.x Status wins over 4.x.x when no Action is present", () => {
    expect(isTransientDeliveryReport({}, "Status: 4.4.1\nStatus: 5.1.1\n")).toBe(false);
  });

  it("a delay that references none of our sends is still unrelated", () => {
    expect(classifyInbound(DELAY_HEADERS, GMAIL_DELAY, new Set()).kind).toBe("unrelated");
  });
});

describe("planDelayedDsnBackfill", () => {
  const row = (id: string, campaign: string, text: string, at: string, step = 1): DsnRow => ({
    id,
    instantlyCampaignId: campaign,
    step,
    headers: text === ADDRESS_NOT_FOUND ? FAILURE_HEADERS : DELAY_HEADERS,
    text,
    at: new Date(at),
  });
  const ev = (id: string, campaign: string, sourceRowId: string): BouncedEvent => ({
    id,
    campaignId: campaign,
    leadEmail: `${campaign}@lead.com`,
    step: 1,
    sourceRowId,
  });

  const rows = [
    row("d1", "c1", GMAIL_DELAY, "2026-08-13T13:12:35Z"),
    row("d2", "c2", GMAIL_DELAY, "2026-08-13T13:12:35Z"),
    row("p2", "c2", ADDRESS_NOT_FOUND, "2026-08-15T12:00:00Z"),
    row("p3", "c3", ADDRESS_NOT_FOUND, "2026-08-15T12:00:00Z"),
  ];

  it("retracts a delay-born bounce with no later permanent failure", () => {
    const plan = planDelayedDsnBackfill(rows, [ev("e1", "c1", "d1")]);
    expect(plan.retract.map((e) => e.id)).toEqual(["e1"]);
    expect(plan.reattribute).toEqual([]);
  });

  it("re-points a delay-born bounce at the permanent failure that followed it", () => {
    const plan = planDelayedDsnBackfill(rows, [ev("e2", "c2", "d2")]);
    expect(plan.retract).toEqual([]);
    expect(plan.reattribute.map((r) => [r.event.id, r.replacement.id])).toEqual([["e2", "p2"]]);
  });

  it("leaves a bounce born from a permanent failure untouched", () => {
    const plan = planDelayedDsnBackfill(rows, [ev("e3", "c3", "p3")]);
    expect(plan.retract).toEqual([]);
    expect(plan.reattribute).toEqual([]);
  });

  it("lists every delay row for reclassification, and only those", () => {
    expect(planDelayedDsnBackfill(rows, []).transientRowIds.sort()).toEqual(["d1", "d2"]);
  });
});
