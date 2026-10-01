import { describe, expect, it } from "vitest";

import { prospectLabel, renderCelebration } from "../../src/lib/celebrate-positive-reply";
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

describe("renderCelebration", () => {
  const history = {
    items: [
      { type: "message" as const, message: sent },
      { type: "message" as const, message: reply },
    ],
    notes: [],
  };

  it("names the person and the brand in the subject", () => {
    const out = renderCelebration({ leadEmail: "dr.k@kineticchiropracticutah.com", brandName: "Shockwavecenters", reply, history });
    expect(out.subject).toBe("Good news: Andrew Kakishita replied to your Shockwavecenters outreach");
    expect(out.html).toContain("Andrew Kakishita wrote back");
  });

  it("shows the reply complete and escaped, and the earlier email once", () => {
    const out = renderCelebration({ leadEmail: "dr.k@kineticchiropracticutah.com", brandName: null, reply, history });
    expect(out.html).toContain("&lt;shockwave&gt; units?");
    expect(out.html).not.toContain("<shockwave>");
    expect(out.text).toContain(reply.bodyText);
    expect(out.html.split("Hi Andrew,").length - 1).toBe(1);
  });

  it("says so when the reply could not be read, and never summarizes it", () => {
    const out = renderCelebration({ leadEmail: "x@y.com", brandName: "B", reply: null, history: { items: [], notes: [] } });
    expect(out.html).toContain("We could not read their reply");
    expect(out.subject).toBe("Good news: x@y.com replied to your B outreach");
  });

  it("carries no em-dash in what the client reads", () => {
    const out = renderCelebration({ leadEmail: "dr.k@kineticchiropracticutah.com", brandName: "B", reply, history });
    expect(out.html).not.toContain("—");
    expect(out.text).not.toContain("—");
  });
});
