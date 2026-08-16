import { describe, it, expect } from "vitest";

import {
  buildUnsubscribePath,
  parseSignedUnsubscribe,
  signUnsubscribe,
  verifyUnsubscribeSignature,
} from "../../src/lib/self-send/unsubscribe";
import {
  INSTANTLY_UNSUBSCRIBE_PLACEHOLDER,
  buildFromHeader,
  buildMessage,
  resolveUnsubscribePlaceholder,
  subjectForStep,
} from "../../src/lib/self-send/message";
import {
  classifyDispatchFailure,
  extractReplyCode,
} from "../../src/lib/self-send/smtp";
import type { Account } from "../../src/lib/instantly-client";

const SECRET = "test-secret";
const IDENTITY = { instantlyCampaignId: "camp-1", leadEmail: "prospect@example.com" };

function account(overrides: Partial<Account> = {}): Account {
  return {
    email: "amy@saviolabsco.com",
    warmup_status: 1,
    status: 1,
    first_name: "Amy",
    last_name: "Moore",
    ...overrides,
  } as Account;
}

// ─── Unsubscribe signing ──────────────────────────────────────────────────────

describe("unsubscribe signing", () => {
  it("round-trips an identity through the signed path", () => {
    const path = buildUnsubscribePath(IDENTITY, SECRET);
    const [, , payload, signature] = path.split("/");

    expect(parseSignedUnsubscribe(payload!, signature!, SECRET)).toEqual(IDENTITY);
  });

  it("produces a URL-safe path with no base64 padding or slashes", () => {
    const path = buildUnsubscribePath(
      { instantlyCampaignId: "c/c+c", leadEmail: "a+b@example.com" },
      SECRET,
    );
    const segments = path.split("/");

    expect(segments).toHaveLength(4);
    expect(segments[2]).not.toMatch(/[+/=]/);
    expect(segments[3]).not.toMatch(/[+/=]/);
  });

  it("normalises email casing so the same recipient always signs identically", () => {
    expect(signUnsubscribe(IDENTITY, SECRET)).toBe(
      signUnsubscribe({ ...IDENTITY, leadEmail: "PROSPECT@Example.COM" }, SECRET),
    );
  });

  it("rejects a signature minted with a different secret", () => {
    const signature = signUnsubscribe(IDENTITY, "other-secret");
    expect(verifyUnsubscribeSignature(IDENTITY, signature, SECRET)).toBe(false);
  });

  it("rejects a tampered identity carrying a valid signature for another one", () => {
    const signature = signUnsubscribe(IDENTITY, SECRET);
    expect(
      verifyUnsubscribeSignature(
        { ...IDENTITY, leadEmail: "someone-else@example.com" },
        signature,
        SECRET,
      ),
    ).toBe(false);
  });

  // A length mismatch must not throw out of timingSafeEqual.
  it("rejects a wrong-length signature without throwing", () => {
    expect(() => verifyUnsubscribeSignature(IDENTITY, "short", SECRET)).not.toThrow();
    expect(verifyUnsubscribeSignature(IDENTITY, "short", SECRET)).toBe(false);
  });

  it.each([
    ["not-base64!!", "sig"],
    ["", ""],
    [Buffer.from("no-separator").toString("base64url"), "sig"],
    [Buffer.from(":leading").toString("base64url"), "sig"],
    [Buffer.from("trailing:").toString("base64url"), "sig"],
  ])("returns null for a malformed payload (%p) rather than throwing", (payload, sig) => {
    expect(() => parseSignedUnsubscribe(payload, sig, SECRET)).not.toThrow();
    expect(parseSignedUnsubscribe(payload, sig, SECRET)).toBeNull();
  });

  it("keeps an email containing a colon intact by splitting from the right", () => {
    const odd = { instantlyCampaignId: "camp:with:colons", leadEmail: "a@b.com" };
    const path = buildUnsubscribePath(odd, SECRET);
    const [, , payload, signature] = path.split("/");

    expect(parseSignedUnsubscribe(payload!, signature!, SECRET)).toEqual(odd);
  });
});

// ─── Message building ─────────────────────────────────────────────────────────

describe("resolveUnsubscribePlaceholder", () => {
  it("replaces every occurrence, so no dead link can ship beside a live one", () => {
    const html = `<a href="${INSTANTLY_UNSUBSCRIBE_PLACEHOLDER}">a</a><a href="${INSTANTLY_UNSUBSCRIBE_PLACEHOLDER}">b</a>`;
    const out = resolveUnsubscribePlaceholder(html, "https://x.test/u/p/s");

    expect(out).not.toContain(INSTANTLY_UNSUBSCRIBE_PLACEHOLDER);
    expect(out.match(/https:\/\/x\.test/g)).toHaveLength(2);
  });
});

describe("subjectForStep", () => {
  it("leaves step 1 alone", () => {
    expect(subjectForStep("Quick question", 1)).toBe("Quick question");
  });

  it("prefixes a followup so the thread stays collapsed", () => {
    expect(subjectForStep("Quick question", 2)).toBe("Re: Quick question");
  });

  it.each([["Re: Quick question"], ["RE: Quick question"], ["  re: Quick question"]])(
    "does not double an existing Re: on %p",
    (subject) => {
      expect(subjectForStep(subject, 3)).toBe(subject);
    },
  );
});

describe("buildFromHeader", () => {
  it("uses the account's own name so From and signature agree", () => {
    expect(buildFromHeader(account())).toBe('"Amy Moore" <amy@saviolabsco.com>');
  });

  it("falls back to the bare address when the account has no name", () => {
    expect(buildFromHeader(account({ first_name: undefined, last_name: undefined }))).toBe(
      "amy@saviolabsco.com",
    );
  });

  // An unescaped quote would split the header into a second field.
  it("escapes quotes and backslashes in a persona name", () => {
    const header = buildFromHeader(account({ first_name: 'A"B\\C', last_name: undefined }));
    expect(header).toBe('"A\\"B\\\\C" <amy@saviolabsco.com>');
  });
});

describe("buildMessage", () => {
  const URL = "https://opt.test/u/payload/sig";

  function build(overrides: Record<string, unknown> = {}) {
    return buildMessage({
      account: account(),
      leadEmail: "prospect@example.com",
      subject: "Quick question",
      bodyHtml: "<p>Hi there</p>",
      step: 1,
      identity: IDENTITY,
      unsubscribeUrl: URL,
      ...overrides,
    });
  }

  it("ships no Instantly placeholder — only Instantly resolves that", () => {
    const message = build();
    expect(message.html).not.toContain(INSTANTLY_UNSUBSCRIBE_PLACEHOLDER);
    expect(message.html).toContain(URL);
  });

  it("keeps the existing signature block rather than reimplementing it", () => {
    const message = build();
    expect(message.html).toContain("<p>--</p>");
    expect(message.html).toContain("Amy Moore");
    expect(message.html).toContain("Distribute.you | Marketing Agency");
  });

  // Same guarantee the Instantly path has: a re-sent body never stacks signatures.
  it("stays idempotent when the body already carries a signature block", () => {
    const once = build().html;
    const twice = build({ bodyHtml: once }).html;
    expect(twice).toBe(once);
  });

  it("sets the RFC 8058 one-click pair pointing at the same URL", () => {
    const message = build();
    expect(message.headers["List-Unsubscribe"]).toBe(`<${URL}>`);
    expect(message.headers["List-Unsubscribe-Post"]).toBe("List-Unsubscribe=One-Click");
  });

  it("threads a followup onto the previous step", () => {
    const message = build({
      step: 2,
      previousMessageId: "<step1@mail>",
      priorMessageIds: ["<step1@mail>"],
    });

    expect(message.subject).toBe("Re: Quick question");
    expect(message.inReplyTo).toBe("<step1@mail>");
    expect(message.references).toEqual(["<step1@mail>"]);
  });

  it("omits the threading headers entirely on the first step", () => {
    const message = build();
    expect(message.inReplyTo).toBeUndefined();
    expect(message.references).toBeUndefined();
  });
});

// ─── SMTP failure classification ──────────────────────────────────────────────

describe("extractReplyCode", () => {
  it("prefers the client's own responseCode", () => {
    expect(extractReplyCode({ responseCode: 550, response: "550 blocked" })).toBe(550);
  });

  it("falls back to a leading reply code in the raw response", () => {
    expect(extractReplyCode({ response: "550-5.4.5 Daily user sending limit exceeded" })).toBe(550);
    expect(extractReplyCode({ response: "450 4.1.2 Domain not found" })).toBe(450);
  });

  it("falls back to an RFC 3463 enhanced code when no basic code is present", () => {
    expect(extractReplyCode({ response: "Rejected 5.7.1 policy" })).toBe(500);
  });

  it("returns null for a socket error carrying no reply at all", () => {
    expect(extractReplyCode(new Error("ECONNRESET"))).toBeNull();
  });
});

describe("classifyDispatchFailure", () => {
  it("treats a 5xx as permanent — the server really refused", () => {
    expect(classifyDispatchFailure({ responseCode: 550 })).toBe("permanent");
  });

  it("treats a 4xx as transient — greylisting and rate limits come back", () => {
    expect(classifyDispatchFailure({ responseCode: 450 })).toBe("transient");
    expect(classifyDispatchFailure({ responseCode: 421 })).toBe("transient");
  });

  // Defaulting an unknown failure to permanent would cancel a lead's remaining
  // steps, and its holds, on what may have been nothing but a dropped socket.
  it("defaults an unclassifiable failure to transient", () => {
    expect(classifyDispatchFailure(new Error("socket hang up"))).toBe("transient");
    expect(classifyDispatchFailure(null)).toBe("transient");
  });
});
