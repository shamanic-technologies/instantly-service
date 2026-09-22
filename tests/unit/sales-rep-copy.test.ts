/**
 * WHO gets copied when a prospect says they are interested.
 *
 * Two sends copy the client's sales rep — the forward of the thread, and the
 * one-to-one reply we send back into it. These pin the rule they share, and the
 * two ways it is allowed to produce nobody: a brand that stated no rep (the
 * common case, 185 of 188 brands) and a brand-service we could not reach.
 */

import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";

const mockGetSalesRep = vi.fn();

vi.mock("../../src/lib/brand-client", () => ({
  getSalesRep: (...a: unknown[]) => mockGetSalesRep(...a),
}));

import { salesRepCopyList } from "../../src/lib/sales-rep-copy";
import { replyCcList } from "../../src/lib/reply-to-lead";

describe("salesRepCopyList", () => {
  beforeEach(() => {
    mockGetSalesRep.mockReset();
  });

  it("copies the rep the brand stated", async () => {
    mockGetSalesRep.mockResolvedValue({ email: "dev@docdinners.com", phone: "+17585187473" });
    await expect(salesRepCopyList("brand-1", "org-1")).resolves.toEqual([
      "dev@docdinners.com",
    ]);
    expect(mockGetSalesRep).toHaveBeenCalledWith("brand-1", "org-1");
  });

  it("copies NOBODY when the brand stated no rep — the common case, and not an error", async () => {
    mockGetSalesRep.mockResolvedValue({ email: null, phone: null });
    await expect(salesRepCopyList("brand-1", "org-1")).resolves.toEqual([]);
  });

  it("copies nobody for a rep stated before the email existed, and still reads their phone as none of its business", async () => {
    mockGetSalesRep.mockResolvedValue({ email: null, phone: "+33770657585" });
    await expect(salesRepCopyList("brand-1", "org-1")).resolves.toEqual([]);
  });

  it("asks nothing at all when there is no brand or no org — a platform send carries neither", async () => {
    await expect(salesRepCopyList(null, "org-1")).resolves.toEqual([]);
    await expect(salesRepCopyList("brand-1", null)).resolves.toEqual([]);
    await expect(salesRepCopyList(undefined, undefined)).resolves.toEqual([]);
    expect(mockGetSalesRep).not.toHaveBeenCalled();
  });

  it("a brand-service we cannot reach copies nobody, LOUDLY — and never fails the send", async () => {
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    mockGetSalesRep.mockRejectedValue(new Error("brand-service 500"));

    await expect(salesRepCopyList("brand-1", "org-1")).resolves.toEqual([]);
    expect(err).toHaveBeenCalled();

    err.mockRestore();
  });
});

describe("replyCcList", () => {
  it("keeps the agency inbox when the brand stated no rep", () => {
    expect(replyCcList("kevin@distribute.you", [])).toBe("kevin@distribute.you");
  });

  it("the rep JOINS the agency inbox, never replaces it", () => {
    expect(replyCcList("kevin@distribute.you", ["dev@docdinners.com"])).toBe(
      "kevin@distribute.you,dev@docdinners.com",
    );
  });

  it("names one mailbox once, whatever case it was written in", () => {
    expect(replyCcList("kevin@distribute.you", ["KEVIN@Distribute.You"])).toBe(
      "kevin@distribute.you",
    );
  });

  it("drops a blank rather than putting an empty address on the header", () => {
    expect(replyCcList("kevin@distribute.you", ["  "])).toBe("kevin@distribute.you");
  });
});
