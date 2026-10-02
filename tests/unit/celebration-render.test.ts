import { describe, expect, it } from "vitest";

import {
  celebrationVariantFor,
  conversationHref,
  prospectLabel,
  renderCelebration,
} from "../../src/lib/celebrate-positive-reply";
import type { ThreadMessage } from "../../src/lib/forward-positive-reply";

const reply: ThreadMessage = {
  direction: "inbound",
  from: '"Andrew Kakishita" <dr.k@kineticchiropracticutah.com>',
  to: "kevin.l@maildistribute.com",
  date: "2026-09-30T18:25:00.000Z",
  subject: "Re: Kinetic Chiropractic shockwave",
  bodyText: "Hey Kevin,\n\nIs this email meant for those who don't have <shockwave> units?\n\nAndrew",
};

describe("prospectLabel", () => {
  it("uses the display name of the reply's From header", () => {
    expect(prospectLabel("dr.k@kineticchiropracticutah.com", reply)).toBe("Andrew Kakishita");
  });
  it("falls back to the address when the From carries no name", () => {
    expect(prospectLabel("a@b.com", { ...reply, from: "a@b.com" })).toBe("a@b.com");
    expect(prospectLabel("a@b.com", { ...reply, from: "<a@b.com>" })).toBe("a@b.com");
    expect(prospectLabel("a@b.com", null)).toBe("a@b.com");
  });
  it("never takes an address-shaped 'name' as a name", () => {
    expect(prospectLabel("a@b.com", { ...reply, from: '"a@b.com" <a@b.com>' })).toBe("a@b.com");
  });
});

const URL = "https://dashboard.distribute.you/v2/orgs/org_3Jv0/brands/brand-1/people/row-1";

describe("renderCelebration (short: title, one line, their reply, one button)", () => {
  const base = {
    leadEmail: "dr.k@kineticchiropracticutah.com",
    brandName: "Shockwavecenters",
    company: "Kinetic Chiropractic",
    reply,
    conversationUrl: URL,
  };

  it("a MEETING request: the party emoji", () => {
    const out = renderCelebration({ ...base, kind: "lead_meeting_requested" });
    expect(out.subject).toBe("\u{1F389} Andrew Kakishita wants to book a call (Shockwavecenters)");
    expect(out.html).toContain("\u{1F389} Andrew Kakishita wants to book a call");
  });

  it("an INFO request is calm: never the party emoji, never congratulated", () => {
    const out = renderCelebration({ ...base, kind: "lead_info_requested" });
    expect(out.subject).toBe("\u{1F4AC} Andrew Kakishita asked for more information (Shockwavecenters)");
    for (const body of [out.subject, out.html, out.text]) {
      expect(body).not.toContain("Congratulations");
      expect(body).not.toContain("\u{1F389}");
    }
  });

  it("a plain INTEREST sits in between", () => {
    const out = renderCelebration({ ...base, kind: "lead_interested" });
    expect(out.subject).toBe("\u{1F44F} Andrew Kakishita is interested (Shockwavecenters)");
  });

  it("an unknown kind reads as plain interest", () => {
    expect(celebrationVariantFor(undefined)).toBe("interested");
    expect(celebrationVariantFor("lead_info_requested")).toBe("info_requested");
    expect(celebrationVariantFor("lead_meeting_requested")).toBe("meeting_requested");
  });

  for (const kind of ["lead_meeting_requested", "lead_info_requested", "lead_interested"]) {
    it(`${kind}: one line, the reply verbatim, one button, nothing else`, () => {
      const out = renderCelebration({ ...base, kind });
      expect(out.text).toContain("Nothing to do, we're answering them.");
      expect(out.html).toContain("Nothing to do, we&#39;re answering them.");
      expect(out.html).toContain(`href="${URL}"`);
      expect(out.html).toContain("Follow the conversation");
      expect(out.text).toContain(reply.bodyText);
      expect(out.html).toContain("&lt;shockwave&gt; units?");
      expect(out.html).not.toContain("<shockwave>");
      // Less to read: no process explanation, no history, no footer.
      for (const gone of ["What happens next", "The conversation so far", "exactly as they wrote it", "Sent by distribute.you", "Hi Andrew,"]) {
        expect(out.html).not.toContain(gone);
      }
      expect(out.text.split("\n").filter((l) => l.trim()).length).toBeLessThanOrEqual(5 + reply.bodyText.split("\n").length);
    });

    it(`${kind}: no em-dash`, () => {
      const out = renderCelebration({ ...base, kind });
      for (const body of [out.subject, out.html, out.text]) expect(body).not.toContain("\u2014");
    });
  }

  it("names the company, then the address, when the reply carries no name", () => {
    const noName = { ...reply, from: "dr.k@kineticchiropracticutah.com" };
    expect(renderCelebration({ ...base, reply: noName, kind: "lead_info_requested" }).subject).toBe(
      "\u{1F4AC} Kinetic Chiropractic asked for more information (Shockwavecenters)",
    );
    expect(renderCelebration({ ...base, reply: noName, company: null, brandName: null, kind: "lead_info_requested" }).subject).toBe(
      "\u{1F4AC} dr.k@kineticchiropracticutah.com asked for more information",
    );
  });

  it("says so when the reply could not be read, and never summarizes it", () => {
    const out = renderCelebration({ ...base, reply: null });
    expect(out.html).toContain("Their reply could not be read here");
  });

  it("escapes the dashboard link", () => {
    const out = renderCelebration({ ...base, conversationUrl: 'https://x/"><script>' });
    expect(out.html).not.toContain('"><script>');
  });
});

describe("conversationHref", () => {
  it("deep-links the person page with the CLERK org id", () => {
    expect(conversationHref({ externalOrgId: "org_3Jv0", brandId: "b-1", leadRowId: "row-1" })).toBe(
      "https://dashboard.distribute.you/v2/orgs/org_3Jv0/brands/b-1/people/row-1",
    );
  });
  it("falls back to the brand's People list without a lead row", () => {
    expect(conversationHref({ externalOrgId: "org_3Jv0", brandId: "b-1", leadRowId: null })).toBe(
      "https://dashboard.distribute.you/v2/orgs/org_3Jv0/brands/b-1/people",
    );
  });
  it("falls back to the dashboard home without the org or the brand", () => {
    expect(conversationHref({ externalOrgId: null, brandId: "b-1", leadRowId: "row-1" })).toBe("https://dashboard.distribute.you/v2");
    expect(conversationHref({ externalOrgId: "org_3Jv0", brandId: null, leadRowId: "row-1" })).toBe("https://dashboard.distribute.you/v2");
  });
});
