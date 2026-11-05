import { describe, it, expect } from "vitest";

import { OUR_OUTREACH_MAX_CHARS, renderOurLatestEmails, renderOurOutreach } from "../../src/lib/self-send/our-outreach";

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

describe("renderOurLatestEmails: 'Our email' is our LAST message before their reply (EVERY-REPLY-LEAD-EVENT-1105)", () => {
  const msg = (direction: "outbound" | "inbound", date: string, bodyText: string) => ({
    direction,
    from: direction === "outbound" ? "kevin@distribute.you" : "james@jcurve.com",
    to: "",
    date,
    subject: "Re: JCurve Solutions <> Distribute.you",
    bodyText,
  });
  // James Barnett, 2026-11-03: he answered Kevin's own mail of 11-02 (filed
  // `staff_reply`), not our automated first email of 11-01.
  const THREAD = [
    msg("outbound", "2026-11-01T22:48:08.000Z", "Automated first email about our offer."),
    msg("outbound", "2026-11-02T05:02:14.000Z", "Hi James, Kevin here. Happy to walk you through it.\n\nOn Sat, Nov 1 wrote:\n> Automated first email"),
    msg("inbound", "2026-11-03T23:39:48.000Z", "I'm currently testing Explee for this."),
    msg("outbound", "2026-11-04T10:00:00.000Z", "An answer after his reply."),
  ];

  it("puts the hand-written mail he answered FIRST, then the earlier ones; nothing sent after the reply", () => {
    const text = renderOurLatestEmails(THREAD, "2026-11-03T23:39:48.000Z")!;
    expect(text.startsWith("Hi James, Kevin here. Happy to walk you through it.")).toBe(true);
    expect(text).toContain("Automated first email about our offer.");
    expect(text.indexOf("Kevin here")).toBeLessThan(text.indexOf("Automated first email about"));
    expect(text).not.toContain("An answer after his reply");
    expect(text).not.toContain("> Automated");
  });

  it("without a reply time, every message of ours counts, newest first", () => {
    expect(renderOurLatestEmails(THREAD, null)!.startsWith("An answer after his reply.")).toBe(true);
  });

  it("is null when we hold no message of ours (falls back to the stored steps)", () => {
    expect(renderOurLatestEmails([THREAD[2]], null)).toBeNull();
  });
});
