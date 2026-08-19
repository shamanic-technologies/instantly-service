import { describe, it, expect } from "vitest";

import {
  buildClickPath,
  isRedirectableUrl,
  parseSignedClick,
  rewriteLinksForTracking,
  signClick,
  type ClickTarget,
} from "../../src/lib/self-send/click-tracking";

const SECRET = "click-test-secret";
const ORIGIN = "https://link.agent-base.ai";

const TARGET: ClickTarget = {
  instantlyCampaignId: "camp-1",
  leadEmail: "prospect@example.com",
  step: 2,
  url: "https://distribute.you/pricing?ref=a&b=c",
};

// ─── Signing ──────────────────────────────────────────────────────────────────

describe("click signing", () => {
  it("round-trips a target through the signed path", () => {
    const [, , payload, signature] = buildClickPath(TARGET, SECRET).split("/");
    expect(parseSignedClick(payload!, signature!, SECRET)).toEqual(TARGET);
  });

  it("survives a target URL full of separators", () => {
    const nasty: ClickTarget = {
      ...TARGET,
      url: "https://x.test/a:b/c?d=1&e=:/%20#frag:ment",
    };
    const [, , payload, signature] = buildClickPath(nasty, SECRET).split("/");
    expect(parseSignedClick(payload!, signature!, SECRET)?.url).toBe(nasty.url);
  });

  it("produces URL-safe segments", () => {
    const segments = buildClickPath(TARGET, SECRET).split("/");
    expect(segments[2]).not.toMatch(/[+/=]/);
    expect(segments[3]).not.toMatch(/[+/=]/);
  });

  // THE security property. Swapping the destination must invalidate the link,
  // otherwise this is an open redirect wearing a signature.
  it("rejects a payload whose destination was swapped", () => {
    const signature = signClick(TARGET, SECRET);
    const tampered = Buffer.from(
      JSON.stringify({
        c: TARGET.instantlyCampaignId,
        l: TARGET.leadEmail,
        s: TARGET.step,
        u: "https://evil.test/phish",
      }),
    )
      .toString("base64url");

    expect(parseSignedClick(tampered, signature, SECRET)).toBeNull();
  });

  it("rejects a signature minted with another secret", () => {
    const [, , payload] = buildClickPath(TARGET, SECRET).split("/");
    expect(parseSignedClick(payload!, signClick(TARGET, "other"), SECRET)).toBeNull();
  });

  it.each([
    ["not-base64!!", "sig"],
    [Buffer.from("not json").toString("base64url"), "sig"],
    [Buffer.from(JSON.stringify({ c: "a" })).toString("base64url"), "sig"],
    [Buffer.from(JSON.stringify({ c: 1, l: 2, s: "x", u: 3 })).toString("base64url"), "sig"],
  ])("returns null for a malformed payload (%p) without throwing", (payload, sig) => {
    expect(() => parseSignedClick(payload, sig, SECRET)).not.toThrow();
    expect(parseSignedClick(payload, sig, SECRET)).toBeNull();
  });

  it("rejects a wrong-length signature without throwing", () => {
    const [, , payload] = buildClickPath(TARGET, SECRET).split("/");
    expect(() => parseSignedClick(payload!, "short", SECRET)).not.toThrow();
    expect(parseSignedClick(payload!, "short", SECRET)).toBeNull();
  });
});

// ─── Redirect safety ──────────────────────────────────────────────────────────

describe("isRedirectableUrl", () => {
  it.each([["https://x.test/a"], ["http://x.test/a"]])("allows %s", (url) => {
    expect(isRedirectableUrl(url)).toBe(true);
  });

  // Bounds the damage if the secret ever leaks: these turn a redirect into
  // script execution in the victim's context.
  it.each([
    ["javascript:alert(1)"],
    ["data:text/html,<script>alert(1)</script>"],
    ["file:///etc/passwd"],
    ["not a url"],
    [""],
  ])("refuses %p", (url) => {
    expect(isRedirectableUrl(url)).toBe(false);
  });
});

// ─── Link rewriting ───────────────────────────────────────────────────────────

describe("rewriteLinksForTracking", () => {
  const identity = {
    instantlyCampaignId: "camp-1",
    leadEmail: "prospect@example.com",
    step: 1,
  };

  const rewrite = (html: string) => rewriteLinksForTracking(html, identity, ORIGIN, SECRET);

  it("routes an outbound link through our redirect", () => {
    const out = rewrite('<a href="https://distribute.you/pricing">Pricing</a>');

    expect(out).toContain(`${ORIGIN}/c/`);
    expect(out).not.toContain('href="https://distribute.you/pricing"');
    expect(out).toContain(">Pricing</a>");
  });

  it("preserves the real destination inside the signed payload", () => {
    const out = rewrite('<a href="https://distribute.you/pricing?x=1">go</a>');
    const [, payload, signature] = /\/c\/([^/"]+)\/([^"]+)"/.exec(out)!;

    expect(parseSignedClick(payload!, signature!, SECRET)?.url).toBe(
      "https://distribute.you/pricing?x=1",
    );
  });

  // The opt-out is the one link a prospect must always be able to trust; burying
  // it behind a tracking hop is both pointless and hostile.
  it("leaves our own opt-out link alone", () => {
    const html = `<a href="${ORIGIN}/u/abc/def">unsubscribe</a>`;
    expect(rewrite(html)).toBe(html);
  });

  it.each([
    ['<a href="mailto:a@b.com">mail</a>'],
    ['<a href="tel:+33123">call</a>'],
    ['<a href="/relative">rel</a>'],
    ['<a href="javascript:alert(1)">x</a>'],
  ])("leaves a non-web href untouched: %s", (html) => {
    expect(rewrite(html)).toBe(html);
  });

  it("rewrites every anchor, not just the first", () => {
    const out = rewrite(
      '<a href="https://a.test">a</a> and <a href="https://b.test">b</a>',
    );
    expect(out.match(new RegExp(`${ORIGIN}/c/`, "g"))).toHaveLength(2);
  });

  it("handles single-quoted hrefs and extra attributes", () => {
    const out = rewrite(`<a class="btn" href='https://a.test' target="_blank">a</a>`);
    expect(out).toContain(`${ORIGIN}/c/`);
    expect(out).toContain('class="btn"');
    expect(out).toContain('target="_blank"');
  });

  it("carries the step, so a click is attributed to the email that caused it", () => {
    const out = rewriteLinksForTracking(
      '<a href="https://a.test">a</a>',
      { ...identity, step: 3 },
      ORIGIN,
      SECRET,
    );
    const [, payload, signature] = /\/c\/([^/"]+)\/([^"]+)"/.exec(out)!;
    expect(parseSignedClick(payload!, signature!, SECRET)?.step).toBe(3);
  });

  it("is a no-op on a body with no links", () => {
    const html = "<p>Hello there</p>";
    expect(rewrite(html)).toBe(html);
  });
});
