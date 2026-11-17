/**
 * POSTMARK-PLACEMENT-1117: Postmark as a seed sending path, and the one-off
 * placement comparison (same email, Postmark vs our mailboxes, same receivers).
 */
import { readFileSync } from "node:fs";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("../../src/lib/key-client", async (orig) => {
  const actual = await orig<typeof import("../../src/lib/key-client")>();
  return { ...actual, resolvePlatformKey: vi.fn() };
});

import { KeyServiceError, resolvePlatformKey } from "../../src/lib/key-client";
import {
  POSTMARK_SEED_PROVIDER,
  PostmarkDispatchError,
  PostmarkSeedServerError,
  TRANSACTIONAL_POSTMARK_SERVER_ID,
  loadPostmarkSeedServer,
  messageIdFromRaw,
  postmarkPayload,
  sendThroughPostmark,
} from "../../src/lib/seed-placement/postmark-transport";
import { bucketOf, planComparisonSends, summarizeComparison, type ComparisonSeedRow } from "../../src/lib/seed-placement/comparison";
import { gmailCategoryOf } from "../../src/lib/seed-placement/classify";
import { buildMessage } from "../../src/lib/self-send/message";
import type { Account } from "../../src/lib/instantly-client";

const caller = { method: "POST", path: "/test" };
const built = buildMessage({
  account: { email: "hello@molthost.org", first_name: "Kevin", last_name: "Lourd" } as Account,
  signatureHtml: null,
  leadEmail: "seed@receiver.com",
  subject: "100 test cases automated free on your apps",
  bodyHtml: "<p>Mekha,</p><p>Your first 100 qualifying test cases are automated free.</p>",
  step: 1,
  identity: { instantlyCampaignId: "cmp:x", leadEmail: "seed@receiver.com" },
  optOut: "mailbox_only",
});

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

describe("postmark payload = the mailbox's email, only the pipe differs", () => {
  it("carries From, To, Subject, the built HTML, every header and our Message-Id, no tracking", () => {
    const payload = postmarkPayload(built, "<abc@molthost.org>");
    expect(payload.From).toBe(built.from);
    expect(payload.To).toBe("seed@receiver.com");
    expect(payload.Subject).toBe(built.subject);
    expect(payload.HtmlBody).toBe(built.html);
    expect(payload.TrackOpens).toBe(false);
    expect(payload.TrackLinks).toBe("None");
    const headers = payload.Headers as { Name: string; Value: string }[];
    expect(headers[0]).toEqual({ Name: "Message-ID", Value: "<abc@molthost.org>" });
    for (const [name, value] of Object.entries(built.headers)) expect(headers).toContainEqual({ Name: name, Value: value });
  });

  it("reads the Message-Id Postmark really sent from the raw dump (folded header included)", () => {
    expect(messageIdFromRaw("From: a\r\nMessage-ID:\r\n <real@mtasv.net>\r\nTo: b\r\n\r\nMessage-ID: <body@x>")).toBe("<real@mtasv.net>");
    expect(messageIdFromRaw("From: a\r\n\r\nbody")).toBeNull();
  });
});

describe("never the transactional server", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    vi.stubGlobal("fetch", fetchMock);
    fetchMock.mockReset();
    vi.mocked(resolvePlatformKey).mockReset();
  });
  afterEach(() => vi.unstubAllGlobals());

  it("refuses when the cold-placement token IS the transactional token", async () => {
    vi.mocked(resolvePlatformKey).mockResolvedValue("same-token");
    await expect(loadPostmarkSeedServer(caller)).rejects.toThrow(PostmarkSeedServerError);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("refuses when the token resolves to server 18354400", async () => {
    vi.mocked(resolvePlatformKey).mockImplementation(async (p) => (p === POSTMARK_SEED_PROVIDER ? "cold" : "tx"));
    fetchMock.mockResolvedValue(jsonResponse(200, { ID: TRANSACTIONAL_POSTMARK_SERVER_ID, Name: "Distribute.you" }));
    await expect(loadPostmarkSeedServer(caller)).rejects.toThrow(/transactional/);
  });

  it("names the missing key when no cold-placement server is configured", async () => {
    vi.mocked(resolvePlatformKey).mockRejectedValue(new KeyServiceError(404, "not found"));
    await expect(loadPostmarkSeedServer(caller)).rejects.toThrow(/postmark-cold-placement/);
  });

  it("accepts a separate server", async () => {
    vi.mocked(resolvePlatformKey).mockImplementation(async (p) => (p === POSTMARK_SEED_PROVIDER ? "cold" : "tx"));
    fetchMock.mockResolvedValue(jsonResponse(200, { ID: 999, Name: "Cold placement test" }));
    await expect(loadPostmarkSeedServer(caller)).resolves.toEqual({ token: "cold", serverId: 999, serverName: "Cold placement test" });
  });

  it("sendThroughPostmark refuses the transactional server id before any call", async () => {
    await expect(sendThroughPostmark({ token: "t", serverId: TRANSACTIONAL_POSTMARK_SERVER_ID, serverName: "" }, built)).rejects.toThrow();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("returns the dump's Message-Id, and a Postmark refusal is a named failure", async () => {
    fetchMock
      .mockResolvedValueOnce(jsonResponse(200, { ErrorCode: 0, MessageID: "pm-1" }))
      .mockResolvedValueOnce(jsonResponse(200, { Body: "Message-ID: <real@molthost.org>\r\n\r\nhi" }));
    const sent = await sendThroughPostmark({ token: "t", serverId: 1, serverName: "" }, built, async () => {});
    expect(sent.messageId).toBe("<real@molthost.org>");

    fetchMock.mockResolvedValueOnce(jsonResponse(422, { ErrorCode: 400, Message: "Sender signature not confirmed" }));
    await expect(sendThroughPostmark({ token: "t", serverId: 1, serverName: "" }, built, async () => {})).rejects.toThrow(PostmarkDispatchError);
  });
});

describe("comparison plan and read", () => {
  it("every sender x receiver x copy, never a self-send", () => {
    const plan = planComparisonSends(
      [
        { email: "hello@molthost.org", transport: "postmark" },
        { email: "a@x.com", transport: "smtp" },
      ],
      [
        { email: "a@x.com", recipientEsp: 1 },
        { email: "r@y.com", recipientEsp: 2 },
      ],
      2,
    );
    expect(plan).toHaveLength(6);
    expect(plan.some((p) => p.sender.email === p.receiver.email)).toBe(false);
  });

  it("Primary/Focused = inbox, another tab/Other = promotions, junk = spam, not found = missing, refused = outside the sample", () => {
    const row = (o: Partial<ComparisonSeedRow>): ComparisonSeedRow => ({
      transport: "postmark", senderEmail: "s", recipientEsp: 1, outcome: "sent", placement: null, gmailCategory: null, msInference: null, ...o,
    });
    expect(bucketOf(row({ placement: "inbox", gmailCategory: "primary" }))).toBe("inbox");
    expect(bucketOf(row({ placement: "inbox", gmailCategory: "promotions" }))).toBe("promotions");
    expect(bucketOf(row({ placement: "inbox", msInference: "other" }))).toBe("promotions");
    expect(bucketOf(row({ placement: "spam" }))).toBe("spam");
    expect(bucketOf(row({}))).toBe("missing");
    const [cell] = summarizeComparison([
      row({ placement: "inbox", gmailCategory: "primary" }),
      row({ placement: "spam" }),
      row({}),
      row({ outcome: "permanent" }),
    ]);
    expect(cell).toMatchObject({ sent: 3, inbox: 1, spam: 1, missing: 1, promotions: 0, failed: 1 });
  });

  it("a Gmail INBOX message in no secondary tab is primary", () => {
    const sets = new Map([["promotions", new Set([7])]] as const);
    expect(gmailCategoryOf(7, sets)).toBe("promotions");
    expect(gmailCategoryOf(8, sets)).toBe("primary");
  });
});

describe("a comparison never reaches what the fleet decides on", () => {
  const src = (p: string) => readFileSync(p, "utf8");
  it("silver promotion, cadence, daily series and alerts read purpose = 'fleet' only", () => {
    expect(src("src/lib/seed-placement/sync.ts")).toMatch(/outcome = 'sent' AND purpose = 'fleet'/);
    expect(src("src/lib/seed-placement/run.ts")).toMatch(/outcome = 'sent' AND purpose = 'fleet'/);
    expect(src("src/lib/seed-placement/daily.ts").match(/d\.purpose = 'fleet'/g)).toHaveLength(2);
    expect(src("src/lib/placement-alert.ts")).toMatch(/d\.purpose = 'fleet'/);
  });
  it("mailbox volume and sender health never count a Postmark seed", () => {
    expect(src("src/lib/recent-send-volume.ts")).toMatch(/s\.transport = 'smtp'/);
    const worker = src("src/lib/self-send/dispatch-worker.ts");
    expect(worker).toMatch(/outcome = 'sent' AND transport = 'smtp'/);
    expect(worker).toMatch(/outcome = 'permanent' AND transport = 'smtp'/);
  });
});
