import { describe, it, expect } from "vitest";
import { EMAIL_TEMPLATES } from "../../src/lib/email-templates";

const layoutOf = (name: string) => EMAIL_TEMPLATES.find((t) => t.name === name)?.layout;

describe("registered email templates", () => {
  it("every template states its layout (an omitted one keeps whatever is stored)", () => {
    for (const t of EMAIL_TEMPLATES) expect(t.layout, t.name).toMatch(/^(brand|none)$/);
  });

  it("the answer-request is a plain email, never wrapped in the brand layout", () => {
    expect(layoutOf("positive-reply-answer-request")).toBe("none");
  });

  it("the client-forwardable thread is plain too", () => {
    expect(layoutOf("positive-reply-forward")).toBe("none");
  });

  it("agency-inbox alerts are plain", () => {
    for (const n of ["campaign-error", "reply-escalation", "reply-handover", "reply-unclassified"]) {
      expect(layoutOf(n), n).toBe("none");
    }
  });

  it("the answer-request and the forwardable thread go person-to-person (no unsubscribe)", () => {
    for (const n of ["positive-reply-answer-request", "positive-reply-forward"]) {
      expect(EMAIL_TEMPLATES.find((t) => t.name === n)?.stream, n).toBe("transactional");
    }
  });

  it("the answer-request comes from Kevin, as signed", () => {
    expect(EMAIL_TEMPLATES.find((t) => t.name === "positive-reply-answer-request")?.from).toBe(
      "Kevin Lourd <growth@distribute.you>",
    );
  });

  it("names are unique", () => {
    const names = EMAIL_TEMPLATES.map((t) => t.name);
    expect(new Set(names).size).toBe(names.length);
  });
});
