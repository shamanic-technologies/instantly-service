import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockExecute(...args) },
}));

const mockOptOuts = vi.fn();
const mockNotAProspects = vi.fn();
vi.mock("../../src/lib/lead-optouts", () => ({
  OPT_OUT_REFUSAL_CODE: "lead_opted_out",
  findStandingOptOuts: (...a: unknown[]) => mockOptOuts(...a),
}));
vi.mock("../../src/lib/not-a-prospect", () => ({
  NOT_A_PROSPECT_REFUSAL_CODE: "lead_not_a_prospect",
  findNotAProspects: (...a: unknown[]) => mockNotAProspects(...a),
}));

const mockNotVerified = vi.fn();
vi.mock("../../src/lib/email-verification-gate", () => ({
  EMAIL_NOT_VERIFIED_REFUSAL_CODE: "email_not_verified",
  findEmailNotVerifiedRefusals: (...a: unknown[]) => mockNotVerified(...a),
}));

import { findSendRefusals } from "../../src/lib/send-refusal";

describe("findSendRefusals — what POST /orgs/send would refuse", () => {
  beforeEach(() => {
    mockExecute.mockReset();
    mockOptOuts.mockReset().mockResolvedValue(new Map());
    mockNotAProspects.mockReset().mockResolvedValue(new Map());
    mockNotVerified.mockReset().mockResolvedValue(new Map());
  });

  it("an address our verifier called invalid: email_not_verified until the verdict may be re-asked", async () => {
    mockNotVerified.mockResolvedValue(new Map([["bad@x.com", new Date("2026-11-13T00:00:00.000Z")]]));
    const out = await findSendRefusals("org-1", ["b1"], ["Bad@X.com", "ok@x.com"]);
    expect(out.get("Bad@X.com")).toEqual({ code: "email_not_verified", until: "2026-11-13T00:00:00.000Z" });
    expect(out.has("ok@x.com")).toBe(false);
  });

  it("a person the brand emailed yesterday: NO refusal, sending is not gated on recent contact (RECONTACT-OFF-1015)", async () => {
    const out = await findSendRefusals("org-1", ["b1"], ["JJohnson@PureWellChiro.com"]);
    expect(out.has("JJohnson@PureWellChiro.com")).toBe(false);
    // no send-history lookup is made at all
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("an opt-out never lapses and outlasts an unverified verdict", async () => {
    mockOptOuts.mockResolvedValue(new Map([["a@b.com", {}]]));
    mockNotVerified.mockResolvedValue(new Map([["a@b.com", new Date("2026-12-01T00:00:00.000Z")]]));
    const out = await findSendRefusals("org-1", ["b1"], ["a@b.com"]);
    expect(out.get("a@b.com")).toEqual({ code: "lead_opted_out", until: null });
  });

  it("not a prospect never lapses and outlasts an unverified verdict", async () => {
    mockNotAProspects.mockResolvedValue(new Map([["a@b.com", { brandId: "b1", replyKind: "lead_is_client" }]]));
    mockNotVerified.mockResolvedValue(new Map([["a@b.com", new Date("2026-12-01T00:00:00.000Z")]]));
    const out = await findSendRefusals("org-1", ["b1"], ["a@b.com"]);
    expect(out.get("a@b.com")).toEqual({ code: "lead_not_a_prospect", until: null });
  });

  it("fails loud on a DB error", async () => {
    mockOptOuts.mockRejectedValue(new Error("db down"));
    await expect(findSendRefusals("org-1", ["b1"], ["a@b.com"])).rejects.toThrow("db down");
  });
});
