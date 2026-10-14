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
  resolveSignatureHtmlForSend,
  signatureTextProblem,
} from "../../src/lib/brand-email-signature";
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
