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
const sent: ThreadMessage = {
  direction: "outbound",
  from: "kevin.l@maildistribute.com",
  to: "dr.k@kineticchiropracticutah.com",
  date: "2026-09-30T14:08:00.000Z",
  subject: "Kinetic Chiropractic shockwave",
  bodyText: "Hi Andrew,",
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

describe("renderCelebration", () => {
  const history = {
    items: [
      { type: "message" as const, message: sent },
      { type: "message" as const, message: reply },
    ],
    notes: [],
  };
  const base = {
    leadEmail: "dr.k@kineticchiropracticutah.com",
    brandName: "Shockwavecenters",
    company: "Kinetic Chiropractic",
    reply,
    history,
    conversationUrl: URL,
  };

  it("a MEETING request gets its own email: the full celebration", () => {
    const out = renderCelebration({ ...base, kind: "lead_meeting_requested" });
    expect(out.subject).toBe("\u{1F389} Kinetic Chiropractic wants to book a call");
    expect(out.html).toContain("Kinetic Chiropractic wants to book a call!");
    expect(out.html).toContain("Congratulations, this is the moment the outreach is for.");
    expect(out.html).toContain("\u{1F389}");
  });

  it("an INFO request is calm: no congratulations, no party emoji", () => {
    const out = renderCelebration({ ...base, kind: "lead_info_requested" });
    expect(out.subject).toBe("\u{1F4AC} Kinetic Chiropractic asked for more information");
    expect(out.html).toContain("Kinetic Chiropractic asked for more information");
    expect(out.html).toContain("Andrew Kakishita at Kinetic Chiropractic replied to your Shockwavecenters outreach and wants to know more.");
    for (const body of [out.subject, out.html, out.text]) {
      expect(body).not.toContain("Congratulations");
      expect(body).not.toContain("the moment the outreach is for");
      expect(body).not.toContain("\u{1F389}");
    }
  });

  it("a plain INTEREST sits in between", () => {
    const out = renderCelebration({ ...base, kind: "lead_interested" });
    expect(out.subject).toBe("\u{1F44F} Kinetic Chiropractic replied with interest to your Shockwavecenters outreach");
    expect(out.html).toContain("Kinetic Chiropractic is interested");
    expect(out.html).not.toContain("Congratulations");
    expect(out.html).not.toContain("\u{1F389}");
  });

  it("an unknown kind reads as plain interest", () => {
    expect(celebrationVariantFor(undefined)).toBe("interested");
    expect(celebrationVariantFor("lead_interested")).toBe("interested");
    expect(celebrationVariantFor("lead_info_requested")).toBe("info_requested");
    expect(celebrationVariantFor("lead_meeting_requested")).toBe("meeting_requested");
  });

  for (const kind of ["lead_meeting_requested", "lead_info_requested", "lead_interested"]) {
    it(`${kind}: tells the client they have nothing to do, and links the conversation`, () => {
      const out = renderCelebration({ ...base, kind });
      for (const body of [out.html, out.text]) {
        expect(body).toContain("You have nothing to do.");
        expect(body).toContain("We answer Andrew Kakishita for you, in the same email thread.");
        expect(body).toContain("If we need any information from you to answer, we will come back to you.");
        expect(body).toContain("Follow the conversation");
      }
      expect(out.html).toContain(`href="${URL}"`);
      expect(out.text).toContain(`Follow the conversation: ${URL}`);
    });

    it(`${kind}: carries no em-dash and never mentions opens`, () => {
      const out = renderCelebration({ ...base, kind });
      for (const body of [out.subject, out.html, out.text]) {
        expect(body).not.toContain("\u2014");
        expect(body.toLowerCase()).not.toMatch(/\bopen(ed|s)?\b(?! the conversation)/);
      }
    });
  }

  it("names nobody it cannot name: an address-only prospect is 'them'", () => {
    const out = renderCelebration({ ...base, leadEmail: "a@b.com", company: null, reply: { ...reply, from: "a@b.com" }, kind: "lead_info_requested" });
    expect(out.subject).toBe("\u{1F4AC} a@b.com asked for more information");
    expect(out.text).toContain("We answer them for you, in the same email thread.");
  });

  it("falls back to the person, then the address, when the company is unknown", () => {
    const named = renderCelebration({ ...base, company: null, kind: "lead_meeting_requested" });
    expect(named.subject).toBe("\u{1F389} Andrew Kakishita wants to book a call");
  });

  it("shows the reply complete and escaped, and the earlier email once", () => {
    const out = renderCelebration({ ...base, brandName: null });
    expect(out.html).toContain("&lt;shockwave&gt; units?");
    expect(out.html).not.toContain("<shockwave>");
    expect(out.text).toContain(reply.bodyText);
    expect(out.html.split("Hi Andrew,").length - 1).toBe(1);
  });

  it("says so when the reply could not be read, and never summarizes it", () => {
    const out = renderCelebration({ ...base, leadEmail: "x@y.com", company: null, reply: null, history: { items: [], notes: [] } });
    expect(out.html).toContain("We could not read their reply");
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
