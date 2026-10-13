import { describe, it, expect } from "vitest";
import { renderTeamEscalation, uniboxHref, UNIBOX_BUTTON_LABEL } from "../../src/lib/notify-brand-team";

const REPLY = {
  direction: "inbound" as const,
  from: '"Elena Staeheli" <elena@biopartner.ch>',
  to: "joshua@send.com",
  date: "2026-10-12T09:00:00.000Z",
  subject: "Re: Psyllium",
  bodyText: "Please write to sortimente@biopartner.ch <today>",
};

const base = {
  memberFirstName: "Ana",
  leadEmail: "elena@biopartner.ch",
  brandName: "Bio Husk",
  reply: REPLY,
  history: { items: [], notes: [] },
  uniboxUrl: "https://dashboard.distribute.you/v2/orgs/org_1/brands/b-1/unibox",
};

describe("the brand's Unibox link", () => {
  it("opens the brand's Unibox under the Clerk org id", () => {
    expect(uniboxHref({ externalOrgId: "org_1", brandId: "b-1" })).toBe(
      "https://dashboard.distribute.you/v2/orgs/org_1/brands/b-1/unibox",
    );
  });

  it("degrades to the dashboard home without an org id or a brand", () => {
    expect(uniboxHref({ externalOrgId: null, brandId: "b-1" })).toBe("https://dashboard.distribute.you/v2");
    expect(uniboxHref({ externalOrgId: "org_1", brandId: null })).toBe("https://dashboard.distribute.you/v2");
  });
});

describe("renderTeamEscalation", () => {
  it("names the person, quotes their words escaped, and carries the button", () => {
    const c = renderTeamEscalation({ ...base, reason: "question" });
    expect(c.subject).toBe("Elena Staeheli asked your Bio Husk team a question");
    expect(c.html).toContain("sortimente@biopartner.ch &lt;today&gt;");
    expect(c.html).toContain(UNIBOX_BUTTON_LABEL);
    expect(c.text).toContain(`${UNIBOX_BUTTON_LABEL}: ${base.uniboxUrl}`);
  });

  it("words a referral and an off-topic reply as what they are", () => {
    expect(renderTeamEscalation({ ...base, reason: "referral" }).text).toContain("pointed you to someone else");
    expect(renderTeamEscalation({ ...base, reason: "off_topic" }).text).toContain("something other than your offer");
    expect(renderTeamEscalation({ ...base, reason: "off_topic" }).subject).toBe(
      "Elena Staeheli replied to your Bio Husk outreach",
    );
  });

  it("never carries a long dash in the copy", () => {
    for (const reason of ["question", "off_topic", "referral"] as const) {
      const c = renderTeamEscalation({ ...base, reason });
      expect(c.subject + c.text).not.toMatch(/[–—]/);
    }
  });
});
