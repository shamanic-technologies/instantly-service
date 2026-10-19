/**
 * MS-SEED-PLACEMENT-1018: Microsoft seed receivers (Graph Inbox/Junk reader,
 * OAuth device code), the per-mailbox Microsoft verdict, the recipient MX read,
 * the Microsoft-recipient mailbox pick, and the stored daily series.
 */

import { describe, it, expect } from "vitest";

import {
  classifyGraphMessage,
  readMicrosoftSeedFolders,
  sclOf,
} from "../../src/lib/seed-placement/microsoft-graph";
import {
  MSA_CONSUMER_TENANT_ID,
  accountKindFor,
  interpretTokenPoll,
  parseDeviceCodeResponse,
  signedInAddresses,
  tenantIdOfToken,
} from "../../src/lib/seed-placement/microsoft-oauth";
import {
  isColdStart,
  mayWriteFirstEmail,
  providerPlacement,
  type ProviderPlacement,
} from "../../src/lib/provider-placement";
import { mailHostForMx, readMailHost } from "../../src/lib/recipient-mail-host";
import { buildSeedPlacementDays } from "../../src/lib/seed-placement/daily";
import { selectDueSteps, type AccountCapacity, type PendingSequence } from "../../src/lib/self-send/dispatch";

const DAY = 86_400_000;

// Headers as Microsoft stamps them on a received message (outlook.com, 2026-10).
const msHeaders = (scl: string) => [
  { name: "Authentication-Results", value: "spf=pass (sender IP is 209.85.220.41) smtp.mailfrom=plainsignalco.com; dkim=pass (signature was verified) header.d=plainsignalco.com;dmarc=pass action=none header.from=plainsignalco.com;compauth=pass reason=100" },
  { name: "X-MS-Exchange-Organization-SCL", value: scl },
];

describe("Graph Inbox / Junk reader", () => {
  it("a seed in the Inbox sorted to Focused is inbox, focused, with its auth and SCL", () => {
    const o = classifyGraphMessage("inbox", {
      internetMessageId: "<abc@plainsignalco.com>",
      inferenceClassification: "focused",
      internetMessageHeaders: msHeaders("1"),
    });
    expect(o).toEqual({
      messageId: "<abc@plainsignalco.com>",
      folder: "inbox",
      placement: "inbox",
      inference: "focused",
      scl: 1,
      spfPass: true,
      dkimPass: true,
      dmarcPass: true,
    });
  });

  it("a seed sorted to Other is still Inbox, recorded as other", () => {
    const o = classifyGraphMessage("inbox", { internetMessageId: "<x@a.com>", inferenceClassification: "other" });
    expect(o).toMatchObject({ placement: "inbox", inference: "other", scl: null });
  });

  it("a seed in Junk is spam, with no Focused/Other label (DefiniteJobs case: SCL 5, auth passing)", () => {
    const o = classifyGraphMessage("junkemail", {
      internetMessageId: "<j@plainsignalco.com>",
      inferenceClassification: "focused",
      internetMessageHeaders: msHeaders("5"),
    });
    expect(o).toMatchObject({ placement: "spam", inference: null, scl: 5, spfPass: true, dmarcPass: true });
  });

  it("a message with no Message-Id cannot be matched to a seed: skipped", () => {
    expect(classifyGraphMessage("inbox", { internetMessageId: null })).toBeNull();
  });

  it("absent auth mechanisms stay null, never false", () => {
    expect(classifyGraphMessage("inbox", { internetMessageId: "<a@b.c>" })).toMatchObject({
      spfPass: null,
      dkimPass: null,
      dmarcPass: null,
    });
  });

  it("SCL falls back to the antispam report", () => {
    expect(
      sclOf({ internetMessageHeaders: [{ name: "X-Forefront-Antispam-Report", value: "CIP:1.2.3.4;CTRY:US;SCL:6;SRV:;IPV:NLI" }] }),
    ).toBe(6);
  });

  it("reads Inbox then Junk, follows nextLink, keeps every page", async () => {
    const calls: string[] = [];
    const pages: Record<string, unknown> = {
      inbox1: { value: [{ internetMessageId: "<1@a>" }], "@odata.nextLink": "https://graph.microsoft.com/v1.0/next-inbox" },
      inbox2: { value: [{ internetMessageId: "<2@a>", inferenceClassification: "other" }] },
      junk: { value: [{ internetMessageId: "<3@a>" }, { internetMessageId: null }] },
    };
    const get = async (_token: string, url: string) => {
      calls.push(url);
      if (url.includes("/mailFolders/inbox/")) return pages.inbox1;
      if (url.includes("next-inbox")) return pages.inbox2;
      if (url.includes("/mailFolders/junkemail/")) return pages.junk;
      throw new Error(`unexpected ${url}`);
    };
    const out = await readMicrosoftSeedFolders("tok", new Date("2026-10-18T00:00:00Z"), get);
    expect(out.map((o) => [o.messageId, o.placement])).toEqual([
      ["<1@a>", "inbox"],
      ["<2@a>", "inbox"],
      ["<3@a>", "spam"],
    ]);
    expect(calls[0]).toContain("receivedDateTime+ge+2026-10-18T00%3A00%3A00.000Z");
    expect(calls).toHaveLength(3);
  });
});

describe("Microsoft device code authorization", () => {
  it("parses the device code answer", () => {
    expect(
      parseDeviceCodeResponse(200, {
        device_code: "DC",
        user_code: "JDUCG5U8",
        verification_uri: "https://microsoft.com/devicelogin",
        expires_in: 900,
        interval: 5,
        message: "To sign in, use a web browser...",
      }),
    ).toMatchObject({ deviceCode: "DC", userCode: "JDUCG5U8", expiresInSeconds: 900, intervalSeconds: 5 });
  });

  it("a refused device code request fails loud with Microsoft's words", () => {
    expect(() =>
      parseDeviceCodeResponse(400, { error: "invalid_client", error_description: "AADSTS700016: app not found" }),
    ).toThrow(/AADSTS700016/);
  });

  it("polling: pending and slow_down keep going, tokens end it, anything else fails", () => {
    expect(interpretTokenPoll(400, { error: "authorization_pending" })).toEqual({ kind: "pending" });
    expect(interpretTokenPoll(400, { error: "slow_down" })).toEqual({ kind: "slow_down" });
    expect(interpretTokenPoll(200, { access_token: "a", refresh_token: "r", expires_in: 3600 })).toEqual({
      kind: "tokens",
      accessToken: "a",
      refreshToken: "r",
      expiresInSeconds: 3600,
    });
    expect(interpretTokenPoll(400, { error: "expired_token", error_description: "gone" })).toMatchObject({
      kind: "failed",
      error: "expired_token",
    });
    expect(interpretTokenPoll(400, { error: "authorization_declined" })).toMatchObject({ kind: "failed" });
  });

  it("a token without a refresh token is a failure (the mailbox would go unreadable in an hour)", () => {
    expect(interpretTokenPoll(200, { access_token: "a" })).toMatchObject({ kind: "failed", error: "no_refresh_token" });
  });

  it("the signed-in mailbox is read from /me (mail or UPN), lowercased", () => {
    expect(signedInAddresses({ mail: null, userPrincipalName: "Kevin.Lourd.Dyou@outlook.com" })).toEqual([
      "kevin.lourd.dyou@outlook.com",
    ]);
  });

  it("consumer vs business from the token tenant, else from the domain", () => {
    expect(accountKindFor(MSA_CONSUMER_TENANT_ID, "x@contoso.com")).toBe("consumer");
    expect(accountKindFor("72f988bf-86f1-41af-91ab-2d7cd011db47", "x@contoso.com")).toBe("business");
    expect(accountKindFor(null, "kevin.lourd.dyou@outlook.com")).toBe("consumer");
    expect(accountKindFor(null, "x@contoso.com")).toBeNull();
    const jwt = `h.${Buffer.from(JSON.stringify({ tid: "t-1" })).toString("base64url")}.s`;
    expect(tenantIdOfToken(jwt)).toBe("t-1");
    expect(tenantIdOfToken("opaque-consumer-token")).toBeNull();
  });
});

describe("Microsoft placement verdict per mailbox", () => {
  const asOf = new Date("2026-10-19T12:00:00Z");
  const fresh = new Date(asOf.getTime() - 2 * DAY);

  const ev = (inboxCount: number, spamCount: number, testedAt: Date = fresh) => ({
    inboxCount,
    spamCount,
    seedTotal: inboxCount + spamCount,
    testedAt,
    testIds: ["t"],
  });
  const verdict = (e: ReturnType<typeof ev> | null) => providerPlacement(e, asOf).verdict;

  it("Inbox at Microsoft = inbox; one Junk out of one = junk", () => {
    expect(verdict(ev(2, 0))).toBe("inbox");
    expect(verdict(ev(0, 1))).toBe("junk");
    expect(verdict(ev(1, 1))).toBe("junk");
  });

  it("a seed nobody found yet is not Junk: unmeasured", () => {
    expect(verdict(ev(0, 0))).toBe("unmeasured");
  });

  it("evidence older than 16 days is unmeasured (the Instantly Outlook rows of 2026-08-29)", () => {
    expect(verdict(ev(1, 0, new Date("2026-08-29T06:00:00Z")))).toBe("unmeasured");
    expect(verdict(null)).toBe("unmeasured");
  });

  it("cold start until one production mailbox is measured Inbox; Junk never writes", () => {
    const p = (v: ProviderPlacement["verdict"]): ProviderPlacement => ({ verdict: v, mostlySpam: false });
    expect(isColdStart([p("unmeasured"), p("junk"), undefined])).toBe(true);
    expect(isColdStart([p("unmeasured"), p("inbox")])).toBe(false);
    expect(mayWriteFirstEmail(p("inbox"), false)).toBe(true);
    expect(mayWriteFirstEmail(p("junk"), true)).toBe(false);
    expect(mayWriteFirstEmail(p("unmeasured"), true)).toBe(true);
    expect(mayWriteFirstEmail(undefined, false)).toBe(false);
  });
});

describe("recipient mail host from MX", () => {
  it("Microsoft 365 tenants are microsoft; Outlook.com and Hotmail are microsoft_consumer (they filter apart)", () => {
    expect(mailHostForMx(["ucl-ac-uk.mail.protection.outlook.com"])).toBe("microsoft");
    expect(mailHostForMx(["zahayr-com.mail.protection.outlook.com."])).toBe("microsoft");
    expect(mailHostForMx(["kevinhearon-com.mail.eo.outlook.com"])).toBe("microsoft");
    expect(mailHostForMx(["outlook-com.olc.protection.outlook.com"])).toBe("microsoft_consumer");
    expect(mailHostForMx(["hotmail-com.olc.protection.outlook.com"])).toBe("microsoft_consumer");
  });

  it("Google, a gateway, and no MX are not microsoft", () => {
    expect(mailHostForMx(["aspmx.l.google.com", "alt1.aspmx.l.google.com"])).toBe("google");
    expect(mailHostForMx(["mx0a-001.pphosted.com"])).toBe("other");
    expect(mailHostForMx([])).toBe("none");
  });

  it("NXDOMAIN reads as no mail; a timeout is recorded as unknown, never as Microsoft", async () => {
    const nx = Object.assign(new Error("queryMx ENOTFOUND"), { code: "ENOTFOUND" });
    const timeout = Object.assign(new Error("queryMx ETIMEOUT"), { code: "ETIMEOUT" });
    expect(await readMailHost("dead.example", async () => Promise.reject(nx))).toMatchObject({ provider: "none", error: null });
    expect(await readMailHost("slow.example", async () => Promise.reject(timeout))).toMatchObject({
      provider: "unknown",
      error: "ETIMEOUT",
    });
    expect(
      await readMailHost("ucl.ac.uk", async () => [
        { exchange: "backup.example.com", priority: 20 },
        { exchange: "ucl-ac-uk.mail.protection.outlook.com", priority: 10 },
      ]),
    ).toMatchObject({ provider: "microsoft", mx: ["ucl-ac-uk.mail.protection.outlook.com", "backup.example.com"] });
  });
});

describe("Microsoft-recipient mailbox pick (selectDueSteps)", () => {
  // Monday 15:00 UTC = 10:00 in the default zone: inside the send window.
  const NOW = new Date("2026-08-17T15:00:00Z");
  const JUNK = "alexander@plainsignalco.com";
  const INBOX = "amy@cleanco.com";
  const UNMEASURED = "bob@newco.com";
  const cap = (accountEmail: string, over: Partial<AccountCapacity> = {}): AccountCapacity => ({
    accountEmail,
    mailbox: accountEmail,
    cap: 50,
    recentSustainedDaily: 50,
    sentToday: 0,
    adoptsFirstEmails: true,
    ...over,
  });
  const seq = (id: string, accountEmail: string, over: Partial<PendingSequence> = {}): PendingSequence => ({
    instantlyCampaignId: id,
    leadEmail: `${id}@prospect.com`,
    accountEmail,
    provisionedSteps: [1, 2, 3],
    lastSentStep: null,
    lastSentAt: null,
    stepDelays: [3, 7, null],
    ...over,
  });

  it("a first email to a Microsoft prospect leaves a Junk mailbox for one Microsoft puts in Inbox", () => {
    const out = selectDueSteps(
      [seq("ms-1", JUNK, { recipientProvider: "microsoft" })],
      [cap(JUNK, { placement: { microsoft: { verdict: "junk", mostlySpam: false } } }), cap(INBOX, { placement: { microsoft: { verdict: "inbox", mostlySpam: false } } })],
      NOW,
    );
    expect(out.heldPlacement).toBe(1);
    expect(out.selected).toEqual([
      expect.objectContaining({ instantlyCampaignId: "ms-1", accountEmail: INBOX, rehomedFrom: JUNK }),
    ]);
  });

  it("a Google prospect still goes out from the Junk-at-Microsoft mailbox (routing, never a demotion)", () => {
    const out = selectDueSteps(
      [seq("g-1", JUNK)],
      [cap(JUNK, { placement: { microsoft: { verdict: "junk", mostlySpam: false } } }), cap(INBOX, { placement: { microsoft: { verdict: "inbox", mostlySpam: false } } })],
      NOW,
    );
    expect(out.heldPlacement).toBe(0);
    expect(out.selected).toEqual([expect.objectContaining({ accountEmail: JUNK })]);
  });

  it("a follow-up to a Microsoft prospect stays on its thread's mailbox", () => {
    const out = selectDueSteps(
      [seq("ms-fu", JUNK, { recipientProvider: "microsoft", provisionedSteps: [2, 3], lastSentStep: 1, lastSentAt: new Date(NOW.getTime() - 5 * DAY) })],
      [cap(JUNK, { placement: { microsoft: { verdict: "junk", mostlySpam: false } } }), cap(INBOX, { placement: { microsoft: { verdict: "inbox", mostlySpam: false } } })],
      NOW,
    );
    expect(out.selected).toEqual([expect.objectContaining({ accountEmail: JUNK, step: 2 })]);
  });

  it("once the fleet has a Microsoft-Inbox mailbox, an unmeasured one no longer writes to Microsoft prospects", () => {
    const out = selectDueSteps(
      [seq("ms-2", UNMEASURED, { recipientProvider: "microsoft" })],
      [cap(UNMEASURED), cap(INBOX, { placement: { microsoft: { verdict: "inbox", mostlySpam: false } } })],
      NOW,
    );
    expect(out.selected).toEqual([expect.objectContaining({ accountEmail: INBOX, rehomedFrom: UNMEASURED })]);
  });

  it("cold start (no Microsoft evidence anywhere): unmeasured mailboxes send as before", () => {
    const out = selectDueSteps([seq("ms-3", UNMEASURED, { recipientProvider: "microsoft" })], [cap(UNMEASURED)], NOW);
    expect(out.heldPlacement).toBe(0);
    expect(out.selected).toEqual([expect.objectContaining({ accountEmail: UNMEASURED })]);
  });

  it("every mailbox Junk at Microsoft: the Microsoft prospect waits, never sent to Junk", () => {
    const out = selectDueSteps(
      [seq("ms-4", JUNK, { recipientProvider: "microsoft" })],
      [cap(JUNK, { placement: { microsoft: { verdict: "junk", mostlySpam: false } } }), cap(UNMEASURED, { placement: { microsoft: { verdict: "junk", mostlySpam: false } } })],
      NOW,
    );
    expect(out.selected).toEqual([]);
    expect(out.heldPlacement).toBe(1);
  });
});

describe("stored daily series shape", () => {
  it("emits every day, Google and Microsoft apart, per vendor; a day without seeds is null, never 0 %", () => {
    const days = buildSeedPlacementDays(
      [
        { day: "2026-10-18", recipientEsp: 1, senderVendor: "gandi", seeds: 10, inbox: 1, spam: 9, missing: 0, inboxFocused: 0, inboxOther: 0 },
        { day: "2026-10-18", recipientEsp: 1, senderVendor: "primeforge", seeds: 10, inbox: 9, spam: 1, missing: 0, inboxFocused: 0, inboxOther: 0 },
        { day: "2026-10-18", recipientEsp: 2, senderVendor: "primeforge", seeds: 4, inbox: 3, spam: 1, missing: 0, inboxFocused: 2, inboxOther: 1 },
      ],
      "2026-10-17",
      "2026-10-18",
    );
    expect(days).toHaveLength(2);
    expect(days[0]).toMatchObject({ day: "2026-10-17", google: { seeds: 0, inboxPct: null }, microsoft: { inboxPct: null } });
    expect(days[1].google).toMatchObject({ seeds: 20, inbox: 10, inboxPct: 50 });
    expect(days[1].microsoft).toMatchObject({ seeds: 4, inboxPct: 75, inboxFocused: 2, inboxOther: 1 });
    expect(days[1].byVendor.map((v) => [v.vendor, v.google.inboxPct])).toEqual([
      ["gandi", 10],
      ["primeforge", 90],
    ]);
  });
});
