import { describe, it, expect, vi, beforeEach } from "vitest";

const mockWhere = vi.fn();
vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: () => ({ where: (...a: unknown[]) => mockWhere(...a) }) }),
  },
}));

import {
  EmailSignatureConflictError,
  normalizeSignatureText,
  renderSignatureHtml,
  fillSenderPlaceholders,
  resolveSignatureHtmlForSend,
  signatureTextProblem,
} from "../../src/lib/brand-email-signature";
import { buildFromHeader, buildMessage } from "../../src/lib/self-send/message";
import {
  buildEmailBodyWithSignature,
  buildReplyBodyWithSignature,
  resolveSignatureBlock,
} from "../../src/lib/send-lead";
import type { Account } from "../../src/lib/instantly-client";

const ACCOUNT = { email: "amy@boostdistribute.com", first_name: "Amy", last_name: "Moore" } as Account;
const CLIENT = renderSignatureHtml("Jane Doe\nHead of Sales, Acme");

beforeEach(() => mockWhere.mockReset());

describe("the client's signature text", () => {
  it("normalizes line endings, trailing spaces and blank edge lines", () => {
    expect(normalizeSignatureText("\n\nJane  \r\nAcme\r\n\n")).toBe("Jane\nAcme");
  });

  it("renders escaped lines in the same <p>…<br>…</p> shape as ours", () => {
    expect(renderSignatureHtml("Jane <CEO>\nA & B")).toBe("<p>Jane &lt;CEO&gt;<br>A &amp; B</p>");
  });

  it("refuses empty, too long, too many lines, and a `--` line", () => {
    expect(signatureTextProblem("  \n ")).toMatch(/empty/);
    expect(signatureTextProblem("x".repeat(1001))).toMatch(/1000/);
    expect(signatureTextProblem(Array(13).fill("a").join("\n"))).toMatch(/12 lines/);
    expect(signatureTextProblem("--\nJane")).toMatch(/--/);
    expect(signatureTextProblem("Jane Doe\nAcme")).toBeNull();
  });
});

describe("the signature an email ends with", () => {
  it("is OURS when the thread pinned none (unchanged for anyone who states nothing)", () => {
    expect(resolveSignatureBlock(ACCOUNT, null)).toBe(
      "<p>Amy Moore<br>Distribute.you | Marketing Agency</p>",
    );
    expect(resolveSignatureBlock(ACCOUNT)).toBe(resolveSignatureBlock(ACCOUNT, null));
  });

  it("is EXACTLY the client's when pinned, on the email and on a reply", () => {
    const email = buildEmailBodyWithSignature("<p>Hi Bob</p>", ACCOUNT, CLIENT);
    const reply = buildReplyBodyWithSignature("<p>Thanks</p>", ACCOUNT, CLIENT);
    for (const body of [email, reply]) {
      expect(body.endsWith(`<p>--</p>${CLIENT}`)).toBe(true);
      expect(body).not.toContain("Distribute.you | Marketing Agency");
      expect(body).not.toContain("Amy Moore");
    }
  });

  it("stays idempotent with a pinned signature (re-built N times, one signature)", () => {
    const once = buildEmailBodyWithSignature("<p>Hi Bob</p>", ACCOUNT, CLIENT);
    expect(buildEmailBodyWithSignature(once, ACCOUNT, CLIENT)).toBe(once);
  });
});

describe("resolveSignatureHtmlForSend", () => {
  it("no brand on the send: ours, no lookup", async () => {
    expect(await resolveSignatureHtmlForSend("org-1", [])).toBeNull();
    expect(mockWhere).not.toHaveBeenCalled();
  });

  it("no brand stated one: ours", async () => {
    mockWhere.mockResolvedValueOnce([]);
    expect(await resolveSignatureHtmlForSend("org-1", ["b1"])).toBeNull();
  });

  it("brands agreeing on one text: that text, rendered", async () => {
    mockWhere.mockResolvedValueOnce([
      { brandId: "b1", signature: "Jane\nAcme" },
      { brandId: "b2", signature: "Jane\nAcme" },
    ]);
    expect(await resolveSignatureHtmlForSend("org-1", ["b1", "b2"])).toBe("<p>Jane<br>Acme</p>");
  });

  it("two different texts: a named conflict, never a silent pick", async () => {
    mockWhere.mockResolvedValueOnce([
      { brandId: "b2", signature: "Bob" },
      { brandId: "b1", signature: "Jane" },
    ]);
    const err = await resolveSignatureHtmlForSend("org-1", ["b1", "b2"]).catch((e) => e);
    expect(err).toBeInstanceOf(EmailSignatureConflictError);
    expect(err.brandIds).toEqual(["b1", "b2"]);
  });
});

describe("sender placeholders: the signature names the mailbox that sends", () => {
  const TEXT = "{first_name} {last_name}, DefiniteJobs\n{sender_email}";
  const RUTH = { email: "ruth@agileconsultco.com", first_name: "Ruth", last_name: "Chandler" } as Account;
  const FALLON = { email: "fallon@boostdistribute.com", first_name: "Fallon", last_name: "Price" } as Account;

  it("accepts the four placeholders at write, refuses any other brace", () => {
    expect(signatureTextProblem(TEXT)).toBeNull();
    expect(signatureTextProblem("{full_name}\nDefiniteJobs")).toBeNull();
    expect(signatureTextProblem("{firstname}\nAcme")).toMatch(/Unknown placeholder `\{firstname\}`/);
    expect(signatureTextProblem("{First_Name}")).toMatch(/Braces are only allowed/);
    expect(signatureTextProblem("Jane {\nAcme")).toMatch(/Braces are only allowed/);
    expect(signatureTextProblem("Jane }")).toMatch(/Braces are only allowed/);
  });

  it("set once through the API (pinned), renders each mailbox's own name, From agreeing", async () => {
    mockWhere.mockResolvedValueOnce([{ brandId: "b1", signature: TEXT }]);
    const pinned = await resolveSignatureHtmlForSend("org-1", ["b1"]);
    expect(pinned).toBe("<p>{first_name} {last_name}, DefiniteJobs<br>{sender_email}</p>");

    const build = (account: Account) =>
      buildMessage({
        account,
        signatureHtml: pinned,
        leadEmail: "prospect@example.com",
        subject: "Quick question",
        bodyHtml: "<p>Hi there</p>",
        step: 1,
        identity: { instantlyCampaignId: "camp-1", leadEmail: "prospect@example.com" },
        unsubscribeUrl: "https://opt.test/u/p/s",
        trackingOrigin: null,
      });
    const ruth = build(RUTH);
    const fallon = build(FALLON);
    expect(ruth.html).toContain("<p>--</p><p>Ruth Chandler, DefiniteJobs<br>ruth@agileconsultco.com</p>");
    expect(fallon.html).toContain("<p>--</p><p>Fallon Price, DefiniteJobs<br>fallon@boostdistribute.com</p>");
    expect(ruth.from).toBe(buildFromHeader(RUTH));
    for (const m of [ruth, fallon]) expect(m.html).not.toMatch(/[{}]/);
  });

  it("fills follow-ups and replies of the thread the same way (one sender, one signature)", () => {
    const pinned = renderSignatureHtml("{full_name}\nDefiniteJobs");
    const email = buildEmailBodyWithSignature("<p>Hi</p>", RUTH, pinned);
    const reply = buildReplyBodyWithSignature("<p>Thanks</p>", RUTH, pinned);
    for (const body of [email, reply]) expect(body.endsWith("<p>--</p><p>Ruth Chandler<br>DefiniteJobs</p>")).toBe(true);
    expect(buildEmailBodyWithSignature(email, RUTH, pinned)).toBe(email);
  });

  it("escapes the mailbox's values like the rest of the signature", () => {
    const odd = { email: "o@x.com", first_name: "Ann <b>", last_name: "O'Neil & Co" } as Account;
    expect(fillSenderPlaceholders("<p>{full_name}</p>", odd)).toBe("<p>Ann &lt;b&gt; O'Neil &amp; Co</p>");
  });

  it("never blocks an email: a mailbox without a name renders without it, no brace left", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const noLast = { email: "x@y.com", first_name: "Amy" } as Account;
    const noName = { email: "x@y.com" } as Account;
    expect(resolveSignatureBlock(noLast, "<p>{first_name} {last_name}, DefiniteJobs<br>{sender_email}</p>")).toBe(
      "<p>Amy, DefiniteJobs<br>x@y.com</p>",
    );
    expect(fillSenderPlaceholders("<p>{full_name} | DefiniteJobs</p>", noName)).toBe("<p>DefiniteJobs</p>");
    expect(fillSenderPlaceholders("<p>{full_name}<br>DefiniteJobs</p>", noName)).toBe("<p>DefiniteJobs</p>");
    expect(fillSenderPlaceholders("<p>{last_name}</p>", noName)).toBe("<p>x@y.com</p>");
    expect(fillSenderPlaceholders("<p>{nope} a { b</p>", RUTH)).toBe("<p>a b</p>");
    const curly = { email: "c@y.com", first_name: "{Amy}", last_name: "Moore" } as Account;
    expect(fillSenderPlaceholders("<p>{full_name}</p>", curly)).toBe("<p>Amy Moore</p>");
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("{last_name} empty for sender x@y.com"));
    warn.mockRestore();
  });

  it("a signature without placeholders is byte-identical to before", () => {
    expect(resolveSignatureBlock(ACCOUNT, CLIENT)).toBe(CLIENT);
    expect(fillSenderPlaceholders(CLIENT, ACCOUNT)).toBe(CLIENT);
  });
});
