import { describe, it, expect } from "vitest";

import {
  familyOf,
  identityKeyOf,
  type CampaignIdentityRow,
} from "../../src/lib/campaign-identity";

const BASE: CampaignIdentityRow = {
  id: "a",
  orgId: "org-1",
  brandId: "brand-1",
  offerId: "offer-1",
  acquisitionChannel: "cold_email",
};

describe("identityKeyOf", () => {
  it("keys on campaign-service's own uniqueness parts", () => {
    const key = identityKeyOf(BASE);
    expect(key).toBe("org-1|brand-1|offer-1||cold_email");
  });

  it("pools rows that differ only by workflow — the whole point", () => {
    expect(identityKeyOf({ ...BASE, id: "a" })).toBe(
      identityKeyOf({ ...BASE, id: "b" }),
    );
  });

  it("reads the legacy brand array when the brand column is absent", () => {
    expect(identityKeyOf({ ...BASE, brandId: null, brandIds: ["brand-1"] })).toBe(
      identityKeyOf(BASE),
    );
  });

  it("an unstated offer pools with its like rather than becoming distinct", () => {
    const a = identityKeyOf({ ...BASE, id: "a", offerId: null });
    const b = identityKeyOf({ ...BASE, id: "b", offerId: undefined });
    expect(a).toBe(b);
    expect(a).not.toBe(identityKeyOf(BASE));
  });

  it("separates two legs of one offer — the owner's key does", () => {
    expect(identityKeyOf({ ...BASE, legKey: "leg-a" })).not.toBe(
      identityKeyOf({ ...BASE, legKey: "leg-b" }),
    );
  });

  it("is null when the row states no brand or no channel — never a guessed identity", () => {
    expect(identityKeyOf({ ...BASE, brandId: null, brandIds: null })).toBeNull();
    expect(identityKeyOf({ ...BASE, acquisitionChannel: null })).toBeNull();
  });
});

describe("familyOf", () => {
  it("returns every row sharing the identity, the asked id included", () => {
    const rows = [
      { ...BASE, id: "c" },
      { ...BASE, id: "a" },
      { ...BASE, id: "b" },
    ];
    expect(familyOf(rows, "a")).toEqual(["a", "b", "c"]);
  });

  it("excludes a row of another offer, brand or channel", () => {
    const rows = [
      { ...BASE, id: "a" },
      { ...BASE, id: "other-offer", offerId: "offer-2" },
      { ...BASE, id: "other-brand", brandId: "brand-2" },
      { ...BASE, id: "other-channel", acquisitionChannel: "pr_cold_email" },
    ];
    expect(familyOf(rows, "a")).toEqual(["a"]);
  });

  it("an unplaceable campaign is a family of one, never folded onto a guess", () => {
    const rows = [
      { ...BASE, id: "a", acquisitionChannel: null },
      { ...BASE, id: "b" },
    ];
    expect(familyOf(rows, "a")).toEqual(["a"]);
  });

  it("a campaign absent from the rows is a family of one", () => {
    expect(familyOf([{ ...BASE, id: "b" }], "a")).toEqual(["a"]);
  });
});
