import { describe, it, expect, vi, beforeEach } from "vitest";

const mockAddCosts = vi.fn();
vi.mock("../../src/lib/runs-client", () => ({
  addCosts: (...a: unknown[]) => mockAddCosts(...a),
}));

import {
  emailSentCostNames,
  provisionStepEmailCosts,
  sendAuthorizeItems,
} from "../../src/lib/send-costs";

describe("the price of an email follows the recipient's verdict (owner 2026-10-14)", () => {
  it("valid = the two base names", () => {
    expect(emailSentCostNames("valid")).toEqual({
      account: "instantly-account-email-sent",
      domain: "instantly-domain-email-sent",
    });
  });
  it("catch_all = the ×2 twins costs-service prices", () => {
    expect(emailSentCostNames("catch_all")).toEqual({
      account: "instantly-account-email-sent-catch-all",
      domain: "instantly-domain-email-sent-catch-all",
    });
  });
  it("unknown = the ×3 twins", () => {
    expect(emailSentCostNames("unknown")).toEqual({
      account: "instantly-account-email-sent-unknown",
      domain: "instantly-domain-email-sent-unknown",
    });
  });
  it.each(["invalid", "risky", ""])("%s has no price: it is never sent", (v) => {
    expect(() => emailSentCostNames(v)).toThrow(/never sent/);
  });

  it("the authorize basket names the verdict's pair for every step", () => {
    expect(sendAuthorizeItems(3, "catch_all")).toEqual([
      { costName: "instantly-account-email-sent-catch-all", quantity: 3 },
      { costName: "instantly-domain-email-sent-catch-all", quantity: 3 },
    ]);
  });

  describe("provisionStepEmailCosts", () => {
    beforeEach(() => mockAddCosts.mockReset());

    it("provisions the verdict's pair on the step run and returns both ids", async () => {
      mockAddCosts.mockResolvedValue({
        costs: [
          { id: "c-acc", costName: "instantly-account-email-sent-unknown" },
          { id: "c-dom", costName: "instantly-domain-email-sent-unknown" },
        ],
      });
      const ids = await provisionStepEmailCosts("run-1", "platform", { orgId: "o", userId: "u" }, "unknown");
      expect(ids).toEqual({ costId: "c-acc", domainCostId: "c-dom" });
      const items = mockAddCosts.mock.calls[0][1] as { costName: string }[];
      expect(items.map((i) => i.costName)).toEqual([
        "instantly-account-email-sent-unknown",
        "instantly-domain-email-sent-unknown",
      ]);
    });

    it("fails loud when runs-service returns another pair", async () => {
      mockAddCosts.mockResolvedValue({
        costs: [
          { id: "c-acc", costName: "instantly-account-email-sent" },
          { id: "c-dom", costName: "instantly-domain-email-sent" },
        ],
      });
      await expect(
        provisionStepEmailCosts("run-1", "platform", { orgId: "o", userId: "u" }, "catch_all"),
      ).rejects.toThrow(/did not return/);
    });
  });
});
