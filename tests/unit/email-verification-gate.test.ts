import { describe, it, expect, vi, beforeEach } from "vitest";

const mockInsertValues = vi.fn();
const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({ values: (v: unknown) => mockInsertValues(v) }),
    execute: (...a: unknown[]) => mockExecute(...a),
  },
}));

const mockVerify = vi.fn();
vi.mock("../../src/lib/apollo-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  verifyEmailAddresses: (...a: unknown[]) => mockVerify(...a),
}));

import {
  findEmailNotVerifiedRefusals,
  gateAddresses,
  gateAddressForSend,
  judgeVerdict,
  refusalUntil,
} from "../../src/lib/email-verification-gate";
import { EmailVerifierError } from "../../src/lib/apollo-client";

const IDENTITY = { orgId: "org-1", userId: "user-1", runId: "run-1" };

function verdictRow(email: string, verdict: string, reused = false) {
  return { email, verdict, verificationId: `ver-${email}`, verifiedAt: "2026-10-14T08:00:00.000Z", reused };
}

describe("judgeVerdict — what the send door does with a verdict", () => {
  it.each(["valid", "catch_all"])("%s is sent", (v) => {
    expect(judgeVerdict(v)).toEqual({ accepted: true, reason: `verdict_${v}` });
  });
  it("a FIRST unknown is not sent: it is re-asked (CATCHALL-BOUNCE-1016)", () => {
    expect(judgeVerdict("unknown")).toEqual({ accepted: false, reason: "verdict_unknown_recheck" });
  });
  it("a CONFIRMED unknown is sent (earlier unknown >= 24 h old, or fresh proof)", () => {
    expect(judgeVerdict("unknown", true)).toEqual({ accepted: true, reason: "verdict_unknown" });
  });
  it("confirmation never rescues a bad verdict", () => {
    expect(judgeVerdict("invalid", true).accepted).toBe(false);
    expect(judgeVerdict("risky", true).accepted).toBe(false);
  });
  it("an unknown refusal lapses after 48 h, a bad verdict after 30 days", () => {
    const at = "2026-10-14T08:00:00.000Z";
    expect(refusalUntil({ verifiedAt: at, reason: "verdict_unknown_recheck" })?.toISOString()).toBe(
      "2026-10-16T08:00:00.000Z",
    );
    expect(refusalUntil({ verifiedAt: at, reason: "verdict_invalid" })?.toISOString()).toBe("2026-11-13T08:00:00.000Z");
  });
  it.each(["invalid", "risky"])("%s is never sent", (v) => {
    expect(judgeVerdict(v)).toEqual({ accepted: false, reason: `verdict_${v}` });
  });
  it("a verdict the code does not know is never sent", () => {
    expect(judgeVerdict("deliverable_maybe")).toEqual({ accepted: false, reason: "verdict_unrecognised" });
  });
});

describe("gateAddresses — verify, decide, record", () => {
  beforeEach(() => {
    mockInsertValues.mockReset().mockResolvedValue(undefined);
    mockVerify.mockReset();
    mockExecute.mockReset();
  });

  it("asks apollo with the sending run's identity and records every decision with its reason", async () => {
    mockVerify.mockResolvedValue([verdictRow("a@x.com", "valid", true), verdictRow("b@x.com", "invalid")]);
    const out = await gateAddresses(["A@x.com", "b@x.com"], IDENTITY, "send");

    expect(mockVerify).toHaveBeenCalledWith(["a@x.com", "b@x.com"], IDENTITY, "instantly-send-gate");
    expect(out.get("a@x.com")).toMatchObject({ accepted: true, reason: "verdict_valid", reused: true });
    expect(out.get("b@x.com")).toMatchObject({ accepted: false, reason: "verdict_invalid", reused: false });
    const rows = mockInsertValues.mock.calls[0][0] as Record<string, unknown>[];
    expect(rows).toHaveLength(2);
    expect(rows[1]).toMatchObject({
      orgId: "org-1",
      email: "b@x.com",
      verdict: "invalid",
      accepted: false,
      reason: "verdict_invalid",
      context: "send",
      runId: "run-1",
    });
  });

  it("an unknown is asked about in our history: confirmed = sent, first = refused and re-asked", async () => {
    mockVerify.mockResolvedValue([verdictRow("a@x.com", "unknown"), verdictRow("b@x.com", "unknown"), verdictRow("c@x.com", "valid")]);
    mockExecute.mockResolvedValue({ rows: [{ email: "a@x.com" }] });
    const out = await gateAddresses(["a@x.com", "b@x.com", "c@x.com"], IDENTITY, "send");

    expect(mockExecute).toHaveBeenCalledTimes(1); // only the unknowns are looked up
    expect(out.get("a@x.com")).toMatchObject({ accepted: true, reason: "verdict_unknown" });
    expect(out.get("b@x.com")).toMatchObject({ accepted: false, reason: "verdict_unknown_recheck" });
    expect(out.get("c@x.com")).toMatchObject({ accepted: true, reason: "verdict_valid" });
  });

  it("no unknown in the answer = no history read", async () => {
    mockVerify.mockResolvedValue([verdictRow("a@x.com", "valid")]);
    await gateAddresses(["a@x.com"], IDENTITY, "send");
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("an unreadable history fails LOUD, never reads as confirmed", async () => {
    mockVerify.mockResolvedValue([verdictRow("a@x.com", "unknown")]);
    mockExecute.mockRejectedValue(new Error("db down"));
    await expect(gateAddressForSend("a@x.com", IDENTITY)).rejects.toThrow(/db down/);
  });

  it("no verdict from the verifier is a RECORDED refusal, never a pass", async () => {
    mockVerify.mockRejectedValue(new EmailVerifierError("502 email_verification", 502, false));
    const d = await gateAddressForSend("a@x.com", IDENTITY);

    expect(d).toMatchObject({ accepted: false, reason: "verifier_failed", verdict: null });
    expect((mockInsertValues.mock.calls[0][0] as Record<string, unknown>[])[0]).toMatchObject({
      accepted: false,
      reason: "verifier_failed",
    });
  });

  it("an org that cannot pay is told apart from an outage", async () => {
    mockVerify.mockRejectedValue(new EmailVerifierError("insufficient balance", 502, true));
    const d = await gateAddressForSend("a@x.com", IDENTITY);
    expect(d.reason).toBe("insufficient_credits");
  });

  it("a sales funnel max-budget refusal relayed by apollo keeps billing's named cause on the decision", async () => {
    const cause = { reason: "sales_funnel_max_budget_reached", sales_funnel_cap: { salesFunnelId: "sf-1" } };
    mockVerify.mockRejectedValue(new EmailVerifierError("402", 402, true, cause));
    const d = await gateAddressForSend("a@x.com", IDENTITY);
    expect(d.reason).toBe("insufficient_credits");
    expect(d.fundingCause).toEqual(cause);
  });

  it("an address missing from apollo's answer is no verdict", async () => {
    mockVerify.mockResolvedValue([]);
    const d = await gateAddressForSend("a@x.com", IDENTITY);
    expect(d).toMatchObject({ accepted: false, reason: "verifier_failed" });
  });

  it("a ledger that cannot be written fails LOUD (a decision we cannot record is not taken)", async () => {
    mockVerify.mockResolvedValue([verdictRow("a@x.com", "valid")]);
    mockInsertValues.mockRejectedValue(new Error("db down"));
    await expect(gateAddressForSend("a@x.com", IDENTITY)).rejects.toThrow(/db down/);
  });

  it("a non-verifier error propagates", async () => {
    mockVerify.mockRejectedValue(new TypeError("boom"));
    await expect(gateAddressForSend("a@x.com", IDENTITY)).rejects.toThrow(/boom/);
  });

  it("splits into apollo's 50-address batches", async () => {
    const emails = Array.from({ length: 120 }, (_, i) => `p${i}@x.com`);
    mockVerify.mockImplementation(async (chunk: string[]) => chunk.map((e) => verdictRow(e, "valid")));
    const out = await gateAddresses(emails, IDENTITY, "queued");
    expect(mockVerify.mock.calls.map((c) => (c[0] as string[]).length)).toEqual([50, 50, 20]);
    expect(out.size).toBe(120);
  });
});

describe("findEmailNotVerifiedRefusals — the status read's refusal", () => {
  beforeEach(() => mockExecute.mockReset());

  it("states a standing refusal on a verdict, skips an accepted latest decision and a lapsed one", async () => {
    const future = new Date(Date.now() + 86_400_000).toISOString();
    const past = new Date(Date.now() - 86_400_000).toISOString();
    mockExecute.mockResolvedValue({
      rows: [
        { email: "bad@x.com", accepted: false, reason: "verdict_invalid", until: future },
        { email: "ok@x.com", accepted: true, reason: "verdict_valid", until: future },
        { email: "old@x.com", accepted: false, reason: "verdict_risky", until: past },
      ],
    });
    const out = await findEmailNotVerifiedRefusals("org-1", ["Bad@x.com", "ok@x.com", "old@x.com"]);
    expect([...out.keys()]).toEqual(["bad@x.com"]);
    expect(out.get("bad@x.com")!.toISOString()).toBe(future);
  });

  it("reads nothing for no addresses", async () => {
    const out = await findEmailNotVerifiedRefusals("org-1", []);
    expect(out.size).toBe(0);
    expect(mockExecute).not.toHaveBeenCalled();
  });
});
