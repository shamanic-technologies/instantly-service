import { describe, it, expect, vi, afterEach } from "vitest";

import {
  fillSenderTokensInBody,
  fillSenderTokensInSubject,
} from "../../src/lib/sender-placeholders";
import {
  buildEmailBodyWithSignature,
  buildReplyBodyWithSignature,
  buildSequenceSteps,
} from "../../src/lib/send-lead";
import { buildMessage } from "../../src/lib/self-send/message";
import { renderSignatureHtml } from "../../src/lib/brand-email-signature";
import type { Account } from "../../src/lib/instantly-client";

function account(overrides: Partial<Account> = {}): Account {
  return {
    email: "kevin@marketingagency.bio",
    warmup_status: 1,
    status: 1,
    first_name: "Kevin",
    last_name: "Lourd",
    ...overrides,
  } as Account;
}

const BRYLEE = account({ email: "brylee@acutezoneco.com", first_name: "Brylee", last_name: "Merrill" });

afterEach(() => vi.restoreAllMocks());

describe("SENDER-NAME-BODY-1111: sender placeholders in the body", () => {
  it("names the mailbox that sends the email, matching the From and the signature", () => {
    const body = "<p>My name is {first_name} and I run growth at Beyond Logic.</p>";
    const pin = renderSignatureHtml("{full_name}\nBeyond Logic");
    const out = buildEmailBodyWithSignature(body, account(), pin);
    expect(out).toContain("<p>My name is Kevin and I run growth at Beyond Logic.</p>");
    expect(out).toContain("<p>Kevin Lourd<br>Beyond Logic</p>");
    expect(out).not.toMatch(/\{(first_name|last_name|full_name|sender_email)\}/);
  });

  it("another mailbox renders its own name (rotation)", () => {
    const body = "<p>I'm {full_name}, write me at {sender_email}.</p>";
    const out = fillSenderTokensInBody(body, BRYLEE);
    expect(out).toBe("<p>I'm Brylee Merrill, write me at brylee@acutezoneco.com.</p>");
  });

  it("a body without placeholders is byte-identical to before (same string back)", () => {
    const body = "<p>Hi {{firstName}}, a {curly} brace and {unsubscribe_link} stay. {{accountSignature}}</p>";
    expect(fillSenderTokensInBody(body, account())).toBe(body);
  });

  it("never touches a double-brace variable that spells a token", () => {
    const body = "<p>{{first_name}} / {first_name}</p>";
    expect(fillSenderTokensInBody(body, account())).toBe("<p>{{first_name}} / Kevin</p>");
  });

  it("HTML-escapes the value in a body", () => {
    const out = fillSenderTokensInBody("<p>{full_name}</p>", account({ first_name: "A<b>", last_name: "O'Neil & Co" }));
    expect(out).toBe("<p>A&lt;b&gt; O'Neil &amp; Co</p>");
  });

  it("a mailbox with no name never blocks: renders empty, tidies the space, warns, no brace left", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const out = fillSenderTokensInBody(
      "<p>Hi there, I am {first_name} {last_name}, from Beyond Logic.</p>",
      account({ first_name: null, last_name: null } as Partial<Account>),
    );
    expect(out).toBe("<p>Hi there, I am, from Beyond Logic.</p>");
    expect(out).not.toMatch(/[{}]/);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("kevin@marketingagency.bio"));
  });

  it("is idempotent through the body builder (f(f(x)) === f(x))", () => {
    const body = "<p>My name is {first_name}.</p>";
    const once = buildEmailBodyWithSignature(body, account());
    expect(buildEmailBodyWithSignature(once, account())).toBe(once);
  });

  it("a reply on the thread renders the same name", () => {
    const out = buildReplyBodyWithSignature("<p>{first_name} here again.</p>", account());
    expect(out).toContain("<p>Kevin here again.</p>");
  });
});

describe("SENDER-NAME-BODY-1111: subject and the built message", () => {
  it("fills the subject as plain text (no escaping)", () => {
    expect(fillSenderTokensInSubject("Note from {first_name} & team", account())).toBe(
      "Note from Kevin & team",
    );
  });

  it("first email and its follow-up from the same mailbox carry the same name", () => {
    const common = {
      account: account(),
      leadEmail: "andy@andyrobinson.coach",
      subject: "{first_name} from Beyond Logic",
      bodyHtml: "<p>My name is {first_name}.</p>",
      identity: { instantlyCampaignId: "self:1", leadEmail: "andy@andyrobinson.coach" },
      optOut: "none" as const,
    };
    const first = buildMessage({ ...common, step: 1 });
    const followUp = buildMessage({ ...common, step: 2, previousMessageId: "<a@b>" });
    expect(first.subject).toBe("Kevin from Beyond Logic");
    expect(followUp.subject).toBe("Re: Kevin from Beyond Logic");
    expect(first.html).toContain("My name is Kevin.");
    expect(followUp.html).toContain("My name is Kevin.");
    expect(first.from).toBe('"Kevin Lourd" <kevin@marketingagency.bio>');
  });

  it("Instantly-transport steps are filled too", () => {
    const [s] = buildSequenceSteps("Hi from {first_name}", [{ step: 1, bodyHtml: "<p>I am {full_name}</p>", daysSinceLastStep: 0 }], account());
    expect(s!.subject).toBe("Hi from Kevin");
    expect(s!.bodyHtml).toContain("I am Kevin Lourd");
  });
});
