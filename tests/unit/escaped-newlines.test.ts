import { describe, it, expect } from "vitest";
import {
  hasEscapedNewlines,
  fixEscapedNewlines,
  planStepFixes,
} from "../../src/lib/escaped-newlines";
import { stripAccountSignature } from "../../src/lib/send-lead";

// A real prod body (campaign 007ee456…, lead joby@littleandlargepubcompany.co.uk,
// delivered 2026-07-30) reduced to its shape: one paragraph carrying literal
// backslash-n, then the signature block and the unsubscribe footer appended by
// buildEmailBodyWithSignature.
const SIG_BLOCK =
  "<p>--</p><p>Kevin Lourd<br />Distribute.you | Marketing Agency</p>" +
  '<p>&nbsp;</p><p style="font-size:12px;color:#999999;font-style:italic">' +
  'Don\'t want to hear from me again? <a href="{unsubscribe_link}" style="color:#999999">unsubscribe</a></p>';

const PROD_BODY =
  "<p>Joby, I represent a team that handles proactive headhunting.\\n\\n" +
  "They specialize in finding elite operational leaders.\\n\\n" +
  "Open to a quick intro?</p>" +
  SIG_BLOCK;

describe("hasEscapedNewlines", () => {
  it("detects a literal backslash-n in the body", () => {
    expect(hasEscapedNewlines(PROD_BODY)).toBe(true);
  });

  it("returns false for a body that only has real newlines", () => {
    expect(hasEscapedNewlines("<p>Hello</p>\n<p>World</p>")).toBe(false);
  });

  it("returns false for a clean single-paragraph body", () => {
    expect(hasEscapedNewlines("<p>Hello there</p>" + SIG_BLOCK)).toBe(false);
  });
});

describe("fixEscapedNewlines", () => {
  it("turns a doubled literal escape into a paragraph break", () => {
    expect(fixEscapedNewlines("<p>A\\n\\nB</p>")).toBe("<p>A</p><p>B</p>");
  });

  it("turns a single literal escape into a line break", () => {
    expect(fixEscapedNewlines("<p>A\\nB</p>")).toBe("<p>A<br>B</p>");
  });

  it("handles a literal carriage-return form the same way", () => {
    expect(fixEscapedNewlines("<p>A\\r\\n\\r\\nB</p>")).toBe("<p>A</p><p>B</p>");
  });

  it("splits the prod body into one paragraph per prospect-facing block", () => {
    const fixed = fixEscapedNewlines(PROD_BODY);
    expect(fixed).toContain(
      "<p>Joby, I represent a team that handles proactive headhunting.</p>",
    );
    expect(fixed).toContain(
      "<p>They specialize in finding elite operational leaders.</p>",
    );
    expect(fixed).toContain("<p>Open to a quick intro?</p>");
  });

  it("leaves no literal escape sequence in the output", () => {
    expect(hasEscapedNewlines(fixEscapedNewlines(PROD_BODY))).toBe(false);
    expect(fixEscapedNewlines(PROD_BODY)).not.toContain("\\n");
  });

  it("preserves the signature block and unsubscribe footer byte-identically", () => {
    expect(fixEscapedNewlines(PROD_BODY)).toContain(SIG_BLOCK);
  });

  it("keeps the paragraph attributes when a styled paragraph is split", () => {
    const styled = '<p style="color:red">A\\n\\nB</p>';
    expect(fixEscapedNewlines(styled)).toBe(
      '<p style="color:red">A</p><p style="color:red">B</p>',
    );
  });

  it("is idempotent — f(f(x)) === f(x)", () => {
    const once = fixEscapedNewlines(PROD_BODY);
    expect(fixEscapedNewlines(once)).toBe(once);
  });

  it("returns a clean body unchanged", () => {
    const clean = "<p>Hello there</p>" + SIG_BLOCK;
    expect(fixEscapedNewlines(clean)).toBe(clean);
  });

  it("degrades to a line break outside any paragraph, never an unbalanced tag", () => {
    const fixed = fixEscapedNewlines("Bare text\\n\\nmore text");
    expect(fixed).toBe("Bare text<br><br>more text");
    expect(fixed).not.toContain("</p>");
  });

  it("drops an empty chunk instead of emitting a blank paragraph", () => {
    expect(fixEscapedNewlines("<p>A\\n\\n\\n\\nB</p>")).toBe("<p>A</p><p>B</p>");
  });

  it("keeps the body strippable by the send path's signature stripper", () => {
    const fixed = fixEscapedNewlines(PROD_BODY);
    const stripped = stripAccountSignature(fixed);
    expect(stripped).not.toContain("Distribute.you | Marketing Agency");
    expect(stripped).toContain("Open to a quick intro?");
  });
});

describe("planStepFixes", () => {
  const dirty = "<p>A\\n\\nB</p>";
  const clean = "<p>A</p><p>B</p>";

  it("rewrites only the steps Instantly has not dispatched yet", () => {
    const plan = planStepFixes(
      [
        { index: 0, body: dirty },
        { index: 1, body: dirty },
        { index: 2, body: dirty },
      ],
      1,
    );
    expect(plan.fixes.map((f) => f.index)).toEqual([1, 2]);
    expect(plan.skippedSentDirty).toEqual([1]);
  });

  it("rewrites every step when nothing has been dispatched", () => {
    const plan = planStepFixes(
      [
        { index: 0, body: dirty },
        { index: 1, body: dirty },
      ],
      0,
    );
    expect(plan.fixes.map((f) => f.index)).toEqual([0, 1]);
    expect(plan.skippedSentDirty).toEqual([]);
  });

  it("plans nothing when no un-sent step is dirty", () => {
    const plan = planStepFixes(
      [
        { index: 0, body: dirty },
        { index: 1, body: clean },
      ],
      1,
    );
    expect(plan.fixes).toEqual([]);
    expect(plan.skippedSentDirty).toEqual([1]);
  });

  it("plans nothing for an entirely clean sequence", () => {
    const plan = planStepFixes(
      [
        { index: 0, body: clean },
        { index: 1, body: clean },
      ],
      0,
    );
    expect(plan.fixes).toEqual([]);
    expect(plan.skippedSentDirty).toEqual([]);
  });

  it("carries the repaired body on each planned fix", () => {
    const plan = planStepFixes([{ index: 0, body: dirty }], 0);
    expect(plan.fixes[0].before).toBe(dirty);
    expect(plan.fixes[0].after).toBe(clean);
  });
});
