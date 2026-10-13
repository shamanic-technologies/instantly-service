import { describe, it, expect } from "vitest";

import { OUR_OUTREACH_MAX_CHARS, renderOurOutreach } from "../../src/lib/self-send/our-outreach";

describe("renderOurOutreach", () => {
  it("renders each sent step as text, oldest first", () => {
    expect(
      renderOurOutreach([
        { subject: "Hiring at Carter & Co this month?", bodyHtml: "Hi Sam,<br><br>Would a free first listing help?" },
        { subject: null, bodyHtml: "<p>Just checking in.</p>" },
      ]),
    ).toBe("Hi Sam,\n\nWould a free first listing help?\n\n---\n\nJust checking in.");
  });

  it("is null when nothing readable was stored", () => {
    expect(renderOurOutreach([])).toBeNull();
    expect(renderOurOutreach([{ subject: "x", bodyHtml: "  " }])).toBeNull();
  });

  it("caps the text: the classifier needs our offer, not the whole sequence", () => {
    const long = "a".repeat(OUR_OUTREACH_MAX_CHARS * 2);
    expect(renderOurOutreach([{ subject: null, bodyHtml: long }])?.length).toBe(OUR_OUTREACH_MAX_CHARS);
  });
});
