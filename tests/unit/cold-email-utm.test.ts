import { describe, it, expect } from "vitest";

import {
  isDistributeHost,
  tagColdEmailLinks,
  withColdEmailUtm,
} from "../../src/lib/cold-email-utm";

describe("withColdEmailUtm", () => {
  it("tags a distribute.you destination as cold email", () => {
    const out = new URL(withColdEmailUtm("https://distribute.you"));
    expect(out.searchParams.get("utm_source")).toBe("cold_email");
    expect(out.searchParams.get("utm_medium")).toBe("cold_email");
  });

  it("tags any distribute.you subdomain, whatever the casing", () => {
    expect(withColdEmailUtm("https://Distribute.you/pricing")).toContain("utm_source=cold_email");
    expect(withColdEmailUtm("https://app.distribute.you/x?y=1")).toBe(
      "https://app.distribute.you/x?y=1&utm_source=cold_email&utm_medium=cold_email",
    );
  });

  it("leaves a customer-domain destination byte-identical", () => {
    for (const url of [
      "https://acme.com/lp?ref=1",
      "https://notdistribute.you/",
      "https://distribute.you.evil.com/",
      "http://ACME.io",
    ]) {
      expect(withColdEmailUtm(url, { content: "step-1" })).toBe(url);
    }
  });

  it("never overwrites a utm param the link already carries", () => {
    const out = new URL(
      withColdEmailUtm("https://distribute.you/?utm_source=partner&utm_content=hero", {
        content: "step-2",
      }),
    );
    expect(out.searchParams.getAll("utm_source")).toEqual(["partner"]);
    expect(out.searchParams.getAll("utm_content")).toEqual(["hero"]);
    expect(out.searchParams.get("utm_medium")).toBe("cold_email");
  });

  it("is idempotent", () => {
    const once = withColdEmailUtm("https://distribute.you/a", { content: "step-1" });
    expect(withColdEmailUtm(once, { content: "step-1" })).toBe(once);
  });

  it("returns unparseable and non-web urls unchanged", () => {
    expect(withColdEmailUtm("mailto:kevin@distribute.you")).toBe("mailto:kevin@distribute.you");
    expect(withColdEmailUtm("not a url")).toBe("not a url");
  });

  it("matches the landing's first-touch classifier", () => {
    // Mirror of distribute.you apps/landing/src/lib/first-touch-script.ts.
    const u = new URL(withColdEmailUtm("https://distribute.you"));
    const src = (u.searchParams.get("utm_source") ?? "").toLowerCase();
    const med = (u.searchParams.get("utm_medium") ?? "").toLowerCase();
    expect(/newsletter/.test(src) || /newsletter/.test(med)).toBe(false);
    expect(/cold.?email|outbound|instantly/.test(src) || /cold.?email|outbound/.test(med)).toBe(
      true,
    );
  });

  it("recognises the host family", () => {
    expect(isDistributeHost("distribute.you")).toBe(true);
    expect(isDistributeHost("www.distribute.you")).toBe(true);
    expect(isDistributeHost("xdistribute.you")).toBe(false);
  });
});

describe("tagColdEmailLinks", () => {
  it("tags distribute.you anchors and leaves customer anchors untouched", () => {
    const html =
      '<p><a href="https://distribute.you">distribute.you</a> and ' +
      '<a href="https://acme.com/x?a=1&b=2">acme</a></p>';
    expect(tagColdEmailLinks(html)).toBe(
      '<p><a href="https://distribute.you/?utm_source=cold_email&utm_medium=cold_email">distribute.you</a> and ' +
        '<a href="https://acme.com/x?a=1&b=2">acme</a></p>',
    );
  });

  it("reads an html-escaped href", () => {
    const out = tagColdEmailLinks('<a href="https://distribute.you/?a=1&amp;b=2">x</a>');
    expect(out).toBe(
      '<a href="https://distribute.you/?a=1&b=2&utm_source=cold_email&utm_medium=cold_email">x</a>',
    );
  });
});
