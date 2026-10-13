import { describe, it, expect } from "vitest";
import { EMAIL_TEMPLATES } from "../../src/lib/email-templates";

const layoutOf = (name: string) => EMAIL_TEMPLATES.find((t) => t.name === name)?.layout;

describe("registered email templates", () => {
  it("every template states its layout (an omitted one keeps whatever is stored)", () => {
    for (const t of EMAIL_TEMPLATES) expect(t.layout, t.name).toMatch(/^(brand|none)$/);
  });

  it("the campaign-error alert is plain", () => {
    expect(layoutOf("campaign-error")).toBe("none");
  });

  it("the client emails (celebration, AI escalation to the brand's team) wear the brand layout", () => {
    for (const n of ["positive-reply-celebration", "reply-escalation"]) expect(layoutOf(n), n).toBe("brand");
  });

  // REPLY-NOTIFS-1013: no staff or client nudge about a reply outside a campaign.
  it("the retired reply nudges are not registered", () => {
    for (const n of [
      "positive-reply-forward",
      "positive-reply-answer-request",
      "reply-handover",
      "reply-unclassified",
      "unanswered-question-check-in",
    ]) {
      expect(layoutOf(n), n).toBeUndefined();
    }
  });

  it("names are unique", () => {
    const names = EMAIL_TEMPLATES.map((t) => t.name);
    expect(new Set(names).size).toBe(names.length);
  });
});
