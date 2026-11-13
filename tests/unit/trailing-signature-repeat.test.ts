import { describe, it, expect, vi } from "vitest";
import {
  signatureLines,
  stripTrailingSignatureRepeat,
} from "../../src/lib/trailing-signature-repeat";
import {
  buildEmailBodyWithSignature,
  buildReplyBodyWithSignature,
} from "../../src/lib/send-lead";
import { sentBodyHtml } from "../../src/lib/self-send/sent-body";
import type { Account } from "../../src/lib/instantly-client";

vi.spyOn(console, "warn").mockImplementation(() => {});

/** The sending mailbox of the Phitilda send (2026-11-13 16:32 UTC, Beyond Logic). */
const kevin = { email: "klourd@clawdhost.tech", first_name: "Kevin", last_name: "Lourd" } as Account;
const PIN = "<p>{full_name}<br>Beyond Logic</p>";

/** The writer's body as recorded in smtp_sent_messages.message_inputs.bodyHtml. */
const PHITILDA_BODY =
  "<p>Hey Phitilda,</p><p>I represent Beyond Logic, we build custom software for ABA and behavioral health agencies at a flat $25,000, delivered in 30 days with full source code ownership.</p><p>Chaim Tesser and his team asked me to reach out to operators in Maryland who might be a good fit for what they do.</p><p>They just shipped RosterABA, a complete staffing platform covering applicant tracking through credentialing to scheduling, in 30 days flat. That's roughly 75% cheaper and about 6x faster than a traditional agency build.</p><p>Worth an intro to Chaim? He can walk you through how it works for agencies dealing with broken intake, credentialing bottlenecks, or authorization tracking.</p><p>{full_name}<br>Beyond Logic</p>";
const PHITILDA_HEAD = PHITILDA_BODY.replace("<p>{full_name}<br>Beyond Logic</p>", "");

const count = (s: string, sub: string) => s.split(sub).length - 1;

describe("DOUBLE-SIGNATURE-1113: a body ending with the signature is signed once", () => {
  it("Phitilda: the writer's {full_name}<br>Beyond Logic is dropped, the appended signature stays", () => {
    const out = buildEmailBodyWithSignature(PHITILDA_BODY, kevin, PIN);
    expect(count(out, "Kevin Lourd<br>Beyond Logic")).toBe(1);
    expect(out.endsWith("<p>--</p><p>Kevin Lourd<br>Beyond Logic</p>")).toBe(true);
    expect(out).toContain("authorization tracking.</p><p>--</p>");
  });

  it("same with the placeholder already filled by the writer", () => {
    const body = PHITILDA_BODY.replace("{full_name}", "Kevin Lourd");
    const out = buildEmailBodyWithSignature(body, kevin, PIN);
    expect(count(out, "Kevin Lourd<br>Beyond Logic")).toBe(1);
  });

  it("same in plain text", () => {
    const body = "Hey Phitilda,\n\nWorth an intro to Chaim?\n\nKevin Lourd\nBeyond Logic\n";
    expect(stripTrailingSignatureRepeat(body, "<p>Kevin Lourd<br>Beyond Logic</p>")).toBe(
      "Hey Phitilda,\n\nWorth an intro to Chaim?",
    );
    const out = buildEmailBodyWithSignature(body.replace("Kevin Lourd", "{full_name}"), kevin, PIN);
    expect(count(out, "Beyond Logic")).toBe(1);
  });

  it("the signature lines inside the last paragraph: the paragraph keeps its other words, HTML stays balanced", () => {
    const body = "<p>Worth a call?</p><p>Best,<br>Kevin Lourd<br>Beyond Logic</p>";
    expect(stripTrailingSignatureRepeat(body, "<p>Kevin Lourd<br>Beyond Logic</p>")).toBe(
      "<p>Worth a call?</p><p>Best,</p>",
    );
  });

  it("our own default signature repeated by the writer is dropped too", () => {
    const body = "<p>Worth a call?</p><p>Kevin Lourd<br>Distribute.you | Marketing Agency</p>";
    const out = buildEmailBodyWithSignature(body, kevin, null);
    expect(count(out, "Distribute.you | Marketing Agency")).toBe(1);
  });

  it("a reply carrying the signature is signed once", () => {
    const out = buildReplyBodyWithSignature("<p>Sure, Tuesday works.</p><p>{full_name}<br>Beyond Logic</p>", kevin, PIN);
    expect(out).toBe("<p>Sure, Tuesday works.</p><p>--</p><p>Kevin Lourd<br>Beyond Logic</p>");
  });

  it("a body that does not END with it is untouched (byte-identical to before)", () => {
    const cases = [
      PHITILDA_HEAD,
      "<p>Kevin Lourd<br>Beyond Logic is the team.</p><p>Worth a call?</p>",
      "<p>Hi,</p><p>Kevin Lourd<br>Beyond Logic</p><p>P.S. one more thing</p>",
      // another name signs this mailbox's email: not the signature being appended
      "<p>Worth a call?</p><p>Amy Moore<br>Beyond Logic</p>",
      // only one of the two lines
      "<p>Worth a call?</p><p>Beyond Logic</p>",
      // the line is only the end of a longer line
      "<p>Worth a call?</p><p>Meet Kevin Lourd<br>Beyond Logic</p>",
    ];
    for (const body of cases) {
      expect(stripTrailingSignatureRepeat(body, "<p>Kevin Lourd<br>Beyond Logic</p>")).toBe(body);
    }
    const out = buildEmailBodyWithSignature(PHITILDA_HEAD, kevin, PIN);
    expect(out).toBe(`${PHITILDA_HEAD}<p>--</p><p>Kevin Lourd<br>Beyond Logic</p>`);
  });

  it("idempotent: f(f(x)) === f(x)", () => {
    const once = buildEmailBodyWithSignature(PHITILDA_BODY, kevin, PIN);
    expect(buildEmailBodyWithSignature(once, kevin, PIN)).toBe(once);
    const twice = PHITILDA_BODY + "<p>Kevin Lourd<br>Beyond Logic</p>";
    const a = buildEmailBodyWithSignature(twice, kevin, PIN);
    expect(count(a, "Beyond Logic</p>")).toBe(1);
    expect(buildEmailBodyWithSignature(a, kevin, PIN)).toBe(a);
  });

  it("history is not rewritten: an unrecorded send before the ship re-renders as it left", () => {
    const row = {
      bodyHtml: PHITILDA_BODY,
      sequenceStep: true,
      pinnedSignatureHtml: PIN,
      accountKnown: true,
      accountEmail: kevin.email,
      accountFirstName: "Kevin",
      accountLastName: "Lourd",
    };
    const before = sentBodyHtml({ ...row, dispatchedAt: "2026-11-13T16:32:00Z" });
    expect(count(before.bodyHtml, "Kevin Lourd<br>Beyond Logic")).toBe(2);
    const after = sentBodyHtml({ ...row, dispatchedAt: "2026-11-15T09:00:00Z" });
    expect(count(after.bodyHtml, "Kevin Lourd<br>Beyond Logic")).toBe(1);
  });

  it("signatureLines splits a block into its text lines", () => {
    expect(signatureLines("<p>{full_name}<br>Beyond Logic</p>")).toEqual(["{full_name}", "Beyond Logic"]);
  });

  it("stays fast on a long body that does not match", () => {
    const body = "<p>x</p>" + "\n".repeat(5000) + "<br>".repeat(2000) + "Kevin Lourd\n\n\n";
    const t = Date.now();
    expect(stripTrailingSignatureRepeat(body, "<p>Kevin Lourd<br>Beyond Logic</p>")).toBe(body);
    expect(Date.now() - t).toBeLessThan(500);
  });
});
